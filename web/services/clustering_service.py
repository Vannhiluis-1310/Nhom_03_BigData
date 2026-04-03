from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.dataset as ds
from pyspark.ml import Pipeline
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import DataFrame, functions as F

from utils.config import PROCESSED_DIR, resolve_model_path
from utils.data_loader import DataLoader
from utils.logger import get_logger
from utils.model_loader import (
    load_clustering_model,
    load_feature_pipeline,
    load_model_metadata,
)
from utils.spark_helper import get_spark

logger = get_logger("clustering_service")


@dataclass
class ClusterResult:
    predictions_pdf: pd.DataFrame
    summary_pdf: pd.DataFrame
    heatmap_pdf: pd.DataFrame
    profiles: dict[int, str]
    metadata: dict[str, Any]
    warning: str | None = None
    total_rows: int = 0
    is_sampled: bool = False


MODEL_LABEL_TO_KEY = {
    "KMeans": "kmeans",
    "BisectingKMeans": "bisecting_kmeans",
    "GaussianMixture": "gaussian_mixture",
}


def _validate_clustering_artifact(model_key: str) -> tuple[Path, bool]:
    """
    Validate and detect clustering artifact structure.

    Returns:
        tuple: (model_path, is_nested)
        - model_path: đường dẫn đến model
        - is_nested: True nếu artifact lưu theo nested structure (data/data/)
    """
    model_path = resolve_model_path("clustering", model_key)

    # Kiểm tra nested structure: data/data/ và data/metadata/
    nested_data_dir = model_path / "data" / "data"
    nested_metadata_dir = model_path / "data" / "metadata"
    is_nested = nested_data_dir.exists() and nested_metadata_dir.exists()

    # Kiểm tra flat structure: data/ và metadata/
    flat_data_dir = model_path / "data"
    flat_metadata_dir = model_path / "metadata"
    is_flat = flat_data_dir.exists() and flat_metadata_dir.exists()

    if not is_nested and not is_flat:
        raise RuntimeError(f"Khong tim thay artifact {model_key} tai: {model_path}")

    return model_path, is_nested


def load_rfm_features() -> DataFrame:
    df = DataLoader.load_spark_df("rfm_customer_features")
    if "recency" in df.columns and "recency_days" not in df.columns:
        df = df.withColumnRenamed("recency", "recency_days")
    if "frequency" in df.columns and "frequency_orders" not in df.columns:
        df = df.withColumnRenamed("frequency", "frequency_orders")
    if "monetary" in df.columns and "monetary_value" not in df.columns:
        df = df.withColumnRenamed("monetary", "monetary_value")

    if "avg_items_per_order" not in df.columns:
        df = df.withColumn("avg_items_per_order", F.lit(1.0))
    if "customer_state" not in df.columns:
        df = df.withColumn("customer_state", F.lit("SP"))

    required = {
        "customer_unique_id",
        "recency_days",
        "frequency_orders",
        "monetary_value",
    }
    missing = required.difference(set(df.columns))
    if missing:
        raise ValueError(f"Thieu cot RFM bat buoc: {sorted(missing)}")
    return df


def _normalize_uploaded_columns(pdf: pd.DataFrame) -> pd.DataFrame:
    out = pdf.copy()
    out.columns = [str(col).strip().lower() for col in out.columns]
    rename_map = {
        "customer_id": "customer_unique_id",
        "customer": "customer_unique_id",
        "recency": "recency_days",
        "frequency": "frequency_orders",
        "monetary": "monetary_value",
    }
    for src, target in rename_map.items():
        if src in out.columns and target not in out.columns:
            out = out.rename(columns={src: target})
    return out


def _prepare_uploaded_raw_df(
    uploaded_pdf: pd.DataFrame,
) -> tuple[DataFrame, str | None]:
    if uploaded_pdf.empty:
        raise ValueError("File upload rong, vui long cung cap du lieu CSV hop le.")

    spark = get_spark()
    normalized = _normalize_uploaded_columns(uploaded_pdf)
    has_id_only = (
        "customer_unique_id" in normalized.columns
        and "recency_days" not in normalized.columns
        and "frequency_orders" not in normalized.columns
        and "monetary_value" not in normalized.columns
    )

    if has_id_only:
        customer_ids = (
            normalized[["customer_unique_id"]]
            .dropna()
            .drop_duplicates()
            .astype({"customer_unique_id": "string"})
        )
        if customer_ids.empty:
            raise ValueError("File upload khong co customer_unique_id hop le.")

        customer_ids_df = spark.createDataFrame(customer_ids)
        matched = load_rfm_features().join(
            customer_ids_df, on="customer_unique_id", how="inner"
        )
        if matched.limit(1).count() == 0:
            raise ValueError(
                "Khong tim thay customer_unique_id nao trong du lieu he thong de phan cum."
            )
        warning = (
            "File chi gom customer_unique_id. Da map voi du lieu RFM hien co "
            "de thuc hien phan cum."
        )
        return matched, warning

    required_cols = {
        "customer_unique_id",
        "recency_days",
        "frequency_orders",
        "monetary_value",
    }
    missing = required_cols.difference(set(normalized.columns))
    if missing:
        raise ValueError(
            "File upload thieu cot bat buoc: "
            + ", ".join(sorted(missing))
            + ". Can it nhat customer_unique_id, recency_days, frequency_orders, monetary_value."
        )

    normalized = normalized.copy()
    if "avg_items_per_order" not in normalized.columns:
        normalized["avg_items_per_order"] = 1.0
    if "customer_state" not in normalized.columns:
        normalized["customer_state"] = "SP"

    numeric_cols = [
        "recency_days",
        "frequency_orders",
        "monetary_value",
        "avg_items_per_order",
    ]
    for col_name in numeric_cols:
        normalized[col_name] = pd.to_numeric(normalized[col_name], errors="coerce")
        normalized[col_name] = normalized[col_name].fillna(0.0)

    normalized["customer_unique_id"] = (
        normalized["customer_unique_id"].astype(str).str.strip()
    )
    normalized["customer_state"] = normalized["customer_state"].fillna("SP").astype(str)

    selected = normalized[
        [
            "customer_unique_id",
            "recency_days",
            "frequency_orders",
            "monetary_value",
            "avg_items_per_order",
            "customer_state",
        ]
    ]
    raw_df = spark.createDataFrame(selected)
    return raw_df, None


def _build_uploaded_features(raw_df: DataFrame) -> tuple[DataFrame, str | None]:
    if "features" in raw_df.columns:
        return raw_df, None

    try:
        feature_df = load_feature_pipeline("clustering").transform(raw_df)
        if "features" in feature_df.columns:
            return feature_df, None
    except Exception as exc:  # noqa: BLE001
        logger.warning("Khong load duoc clustering_fe_pipeline cho upload: %s", exc)

    # Fallback: fit temporary scaler on uploaded sample
    assembler = VectorAssembler(
        inputCols=[
            "recency_days",
            "frequency_orders",
            "monetary_value",
            "avg_items_per_order",
        ],
        outputCol="features_raw",
        handleInvalid="skip",
    )
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withMean=False,
        withStd=True,
    )
    temp_pipeline = Pipeline(stages=[assembler, scaler])
    model = temp_pipeline.fit(raw_df)
    warning = (
        "Khong su dung duoc clustering_fe_pipeline da luu. "
        "Da fallback sang scale tam thoi tren file upload."
    )
    return model.transform(raw_df), warning


def _load_precomputed_clustering_features() -> DataFrame:
    """Load precomputed clustering features to avoid broken saved FE pipeline artifacts."""
    feat_path = PROCESSED_DIR / "features" / "clustering_fe"
    if not feat_path.exists():
        raise FileNotFoundError(f"Khong tim thay clustering_fe tai: {feat_path}")

    spark = get_spark()
    feat_df = spark.read.parquet(str(feat_path))
    required_cols = {"customer_unique_id", "features"}
    missing = required_cols.difference(feat_df.columns)
    if missing:
        raise RuntimeError(
            "clustering_fe thieu cot bat buoc: " + ", ".join(sorted(missing))
        )

    return feat_df.select("customer_unique_id", "features")


def _extract_trained_k(metadata: dict[str, Any]) -> int | None:
    param_map = metadata.get("paramMap", {}) if isinstance(metadata, dict) else {}
    k_val = param_map.get("k") or metadata.get("k")
    if k_val is None:
        return None
    try:
        return int(k_val)
    except (TypeError, ValueError):
        return None


def _build_profiles(summary_pdf: pd.DataFrame) -> dict[int, str]:
    if summary_pdf.empty:
        return {}
    rev_q3 = summary_pdf["avg_revenue"].quantile(0.75)
    freq_q3 = summary_pdf["avg_orders"].quantile(0.75)
    rec_q3 = summary_pdf["avg_recency"].quantile(0.75)
    rec_q2 = summary_pdf["avg_recency"].quantile(0.5)
    freq_q1 = summary_pdf["avg_orders"].quantile(0.25)

    profiles: dict[int, str] = {}
    for _, row in summary_pdf.iterrows():
        cluster = int(row["cluster"])
        if row["avg_revenue"] >= rev_q3 and row["avg_orders"] >= freq_q3:
            profiles[cluster] = "VIP - Gia tri cao va mua thuong xuyen"
        elif row["avg_recency"] >= rec_q3 and row["avg_orders"] <= freq_q1:
            profiles[cluster] = "Churned - Lau khong quay lai"
        elif row["avg_recency"] >= rec_q2:
            profiles[cluster] = "At Risk - Can kich hoat mua lai"
        else:
            profiles[cluster] = "New / Active - Moi hoac dang mua deu"
    return profiles


def _collect_to_pandas(df: DataFrame, limit: int | None = None) -> pd.DataFrame:
    source = df.limit(limit) if limit is not None else df
    rows = source.collect()
    if not rows:
        return pd.DataFrame(columns=source.columns)
    return pd.DataFrame(
        [row.asDict(recursive=True) for row in rows], columns=source.columns
    )


def _vector_struct_to_dense(vec_obj: Any) -> np.ndarray:
    if isinstance(vec_obj, dict):
        vec_type = int(vec_obj.get("type", 1))
        values = vec_obj.get("values") or []
        if vec_type == 1:
            return np.array(values, dtype=float)
        size = int(vec_obj.get("size", len(values)))
        indices = vec_obj.get("indices") or []
        dense = np.zeros(size, dtype=float)
        if indices and values:
            dense[np.array(indices, dtype=int)] = np.array(values, dtype=float)
        return dense
    return np.array([], dtype=float)


def _run_kmeans_offline_prediction(metadata: dict[str, Any]) -> ClusterResult:
    model_path = resolve_model_path("clustering", "kmeans")
    centers_ds = ds.dataset(str(model_path / "data"), format="parquet")
    centers_pdf = centers_ds.to_table(
        columns=["clusterIdx", "clusterCenter"]
    ).to_pandas()
    if centers_pdf.empty:
        raise RuntimeError("Khong doc duoc cluster centers tu model KMeans.")

    centers_pdf = centers_pdf.sort_values("clusterIdx")
    centers = np.vstack(
        [_vector_struct_to_dense(x) for x in centers_pdf["clusterCenter"].tolist()]
    )

    feat_path = PROCESSED_DIR / "features" / "clustering_fe"
    feat_ds = ds.dataset(str(feat_path), format="parquet")
    feat_pdf = feat_ds.to_table(columns=["customer_unique_id", "features"]).to_pandas()
    if feat_pdf.empty:
        raise RuntimeError("Khong doc duoc clustering_fe cho fallback.")

    x_mat = np.vstack(
        [_vector_struct_to_dense(x) for x in feat_pdf["features"].tolist()]
    )
    dist = ((x_mat[:, None, :] - centers[None, :, :]) ** 2).sum(axis=2)
    pred = dist.argmin(axis=1).astype(int)

    base_df = load_rfm_features()
    base_pdf = _collect_to_pandas(base_df)
    merged = base_pdf.merge(
        feat_pdf[["customer_unique_id"]].assign(cluster=pred),
        on="customer_unique_id",
        how="inner",
    )

    summary_pdf = (
        merged.groupby("cluster", as_index=False)
        .agg(
            so_khach_hang=("customer_unique_id", "count"),
            avg_revenue=("monetary_value", "mean"),
            avg_orders=("frequency_orders", "mean"),
            avg_recency=("recency_days", "mean"),
        )
        .sort_values("so_khach_hang", ascending=False)
    )

    heatmap_pdf = (
        summary_pdf[["cluster", "avg_recency", "avg_orders", "avg_revenue"]]
        .rename(
            columns={
                "avg_recency": "Recency",
                "avg_orders": "Frequency",
                "avg_revenue": "Monetary",
            }
        )
        .sort_values("cluster")
    )

    sample_limit = 12000 if len(merged) > 12000 else len(merged)
    is_sampled = len(merged) > sample_limit
    predictions_pdf = merged.head(sample_limit).copy()

    return ClusterResult(
        predictions_pdf=predictions_pdf,
        summary_pdf=summary_pdf,
        heatmap_pdf=heatmap_pdf,
        profiles=_build_profiles(summary_pdf),
        metadata=metadata,
        warning="Dang dung fallback offline KMeans vi Spark worker bi crash.",
        total_rows=len(merged),
        is_sampled=is_sampled,
    )


def _run_bisecting_kmeans_offline_prediction(
    metadata: dict[str, Any], is_nested: bool = False
) -> ClusterResult:
    model_path = resolve_model_path("clustering", "bisecting_kmeans")

    # Xử lý cả nested và flat structure
    if is_nested:
        data_dir = model_path / "data" / "data"
    else:
        data_dir = model_path / "data"

    part_files = sorted(data_dir.glob("part-*.parquet"))
    if not part_files:
        raise RuntimeError("Khong doc duoc cay cluster tu model BisectingKMeans.")

    nodes_ds = ds.dataset([str(p) for p in part_files], format="parquet")
    nodes_pdf = nodes_ds.to_table(columns=["index", "center", "children"]).to_pandas()
    if nodes_pdf.empty:
        raise RuntimeError("Khong co node nao trong artifact BisectingKMeans.")

    centers: dict[int, np.ndarray] = {}
    children_map: dict[int, list[int]] = {}
    for _, row in nodes_pdf.iterrows():
        node_id = int(row["index"])
        centers[node_id] = _vector_struct_to_dense(row["center"])
        children_map[node_id] = [int(child) for child in (row["children"] or [])]

    child_nodes = {child for children in children_map.values() for child in children}
    root_candidates = sorted(set(children_map).difference(child_nodes))
    if not root_candidates:
        raise RuntimeError("Khong xac dinh duoc root node cua BisectingKMeans.")
    root_id = root_candidates[0]

    feat_path = PROCESSED_DIR / "features" / "clustering_fe"
    feat_parts = sorted(feat_path.glob("part-*.parquet"))
    if not feat_parts:
        raise RuntimeError("Khong doc duoc clustering_fe cho fallback BisectingKMeans.")
    feat_ds = ds.dataset([str(p) for p in feat_parts], format="parquet")
    feat_pdf = feat_ds.to_table(columns=["customer_unique_id", "features"]).to_pandas()
    if feat_pdf.empty:
        raise RuntimeError("clustering_fe rong, khong the fallback BisectingKMeans.")

    x_mat = np.vstack(
        [_vector_struct_to_dense(x) for x in feat_pdf["features"].tolist()]
    )

    def assign_cluster(vec: np.ndarray) -> int:
        node_id = root_id
        while True:
            children = children_map.get(node_id) or []
            if not children:
                return node_id
            node_id = min(
                children,
                key=lambda child_id: float(((vec - centers[child_id]) ** 2).sum()),
            )

    pred = np.array([assign_cluster(vec) for vec in x_mat], dtype=int)

    base_pdf = _collect_to_pandas(load_rfm_features())
    merged = base_pdf.merge(
        feat_pdf[["customer_unique_id"]].assign(cluster=pred),
        on="customer_unique_id",
        how="inner",
    )
    if merged.empty:
        raise RuntimeError("Khong ghep duoc ket qua fallback BisectingKMeans vao RFM.")

    summary_pdf = (
        merged.groupby("cluster", as_index=False)
        .agg(
            so_khach_hang=("customer_unique_id", "count"),
            avg_revenue=("monetary_value", "mean"),
            avg_orders=("frequency_orders", "mean"),
            avg_recency=("recency_days", "mean"),
        )
        .sort_values("so_khach_hang", ascending=False)
    )

    heatmap_pdf = (
        summary_pdf[["cluster", "avg_recency", "avg_orders", "avg_revenue"]]
        .rename(
            columns={
                "avg_recency": "Recency",
                "avg_orders": "Frequency",
                "avg_revenue": "Monetary",
            }
        )
        .sort_values("cluster")
    )

    sample_limit = 12000 if len(merged) > 12000 else len(merged)
    is_sampled = len(merged) > sample_limit
    predictions_pdf = merged.head(sample_limit).copy()

    return ClusterResult(
        predictions_pdf=predictions_pdf,
        summary_pdf=summary_pdf,
        heatmap_pdf=heatmap_pdf,
        profiles=_build_profiles(summary_pdf),
        metadata=metadata,
        warning=(
            f"Dang dung fallback offline BisectingKMeans ({'nested' if is_nested else 'flat'} "
            f"artifact) vi Spark khong load duoc artifact model truc tiep."
        ),
        total_rows=len(merged),
        is_sampled=is_sampled,
    )


def _result_from_prediction_df(
    selected_df: DataFrame,
    metadata: dict[str, Any],
    warning: str | None = None,
) -> ClusterResult:
    total_rows = selected_df.count()
    if total_rows == 0:
        return ClusterResult(
            pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}, metadata
        )

    summary_pdf = _collect_to_pandas(
        selected_df.groupBy("prediction")
        .agg(
            F.count("customer_unique_id").alias("so_khach_hang"),
            F.avg("monetary_value").alias("avg_revenue"),
            F.avg("frequency_orders").alias("avg_orders"),
            F.avg("recency_days").alias("avg_recency"),
        )
        .withColumnRenamed("prediction", "cluster")
    ).sort_values("so_khach_hang", ascending=False)

    heatmap_pdf = (
        summary_pdf[["cluster", "avg_recency", "avg_orders", "avg_revenue"]]
        .rename(
            columns={
                "avg_recency": "Recency",
                "avg_orders": "Frequency",
                "avg_revenue": "Monetary",
            }
        )
        .sort_values("cluster")
    )

    sample_limit = 12000 if total_rows > 12000 else total_rows
    predictions_pdf = _collect_to_pandas(selected_df, sample_limit).rename(
        columns={"prediction": "cluster"}
    )

    return ClusterResult(
        predictions_pdf=predictions_pdf,
        summary_pdf=summary_pdf,
        heatmap_pdf=heatmap_pdf,
        profiles=_build_profiles(summary_pdf),
        metadata=metadata,
        warning=warning,
        total_rows=total_rows,
        is_sampled=total_rows > sample_limit,
    )


def run_clustering_prediction(
    model_label: str,
    selected_k: int,
    max_iter: int,
    seed: int,
    raw_df_override: DataFrame | None = None,
    upload_warning: str | None = None,
) -> ClusterResult:
    del max_iter, seed

    model_key = MODEL_LABEL_TO_KEY[model_label]
    model_path, is_nested = _validate_clustering_artifact(model_key)

    try:
        loaded = load_clustering_model(model_key)
    except Exception as exc:  # noqa: BLE001
        msg = str(exc)
        if model_key == "bisecting_kmeans" and raw_df_override is None:
            logger.warning(
                "Khong load duoc artifact BisectingKMeans, dung fallback: %s", exc
            )
            metadata = load_model_metadata(model_path, "clustering", model_key)
            trained_k = _extract_trained_k(metadata)
            if trained_k is not None and selected_k != trained_k:
                return ClusterResult(
                    predictions_pdf=pd.DataFrame(),
                    summary_pdf=pd.DataFrame(),
                    heatmap_pdf=pd.DataFrame(),
                    profiles={},
                    metadata=metadata,
                    warning=(
                        "Chua co model da train cho cau hinh nay. "
                        "Vui long retrain trong trang Admin. "
                        f"(Model hien tai: k={trained_k}, ban chon: k={selected_k})"
                    ),
                )
            return _run_bisecting_kmeans_offline_prediction(metadata, is_nested)
        if (
            "UNABLE_TO_INFER_SCHEMA" in msg
            or "Unable to infer schema for Parquet" in msg
        ):
            raise RuntimeError(
                "Spark khong doc duoc schema Parquet cua artifact clustering. "
                "Thuong do folder model dang sai cau truc save hoac khong tuong thich version Spark. "
                f"Kiem tra va retrain artifact tai: {model_path}"
            ) from exc
        raise

    metadata = loaded.metadata or {}
    trained_k = _extract_trained_k(metadata)
    if trained_k is not None and selected_k != trained_k:
        return ClusterResult(
            predictions_pdf=pd.DataFrame(),
            summary_pdf=pd.DataFrame(),
            heatmap_pdf=pd.DataFrame(),
            profiles={},
            metadata=metadata,
            warning=(
                "Chua co model da train cho cau hinh nay. "
                "Vui long retrain trong trang Admin. "
                f"(Model hien tai: k={trained_k}, ban chon: k={selected_k})"
            ),
        )

    feature_warning: str | None = None
    if raw_df_override is None:
        raw_df = load_rfm_features()
        if "features" in raw_df.columns:
            feature_df = raw_df
        else:
            try:
                precomputed_df = _load_precomputed_clustering_features()
                feature_df = raw_df.join(
                    precomputed_df, on="customer_unique_id", how="inner"
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Khong dung duoc clustering_fe precomputed: %s", exc)
                try:
                    feature_df = load_feature_pipeline("clustering").transform(raw_df)
                except Exception as pipeline_exc:  # noqa: BLE001
                    msg = str(pipeline_exc)
                    if (
                        "UNABLE_TO_INFER_SCHEMA" in msg
                        or "Unable to infer schema for Parquet" in msg
                    ):
                        raise RuntimeError(
                            "Feature pipeline clustering dang hong o stage StandardScaler. "
                            "File parquet trong artifact `clustering_fe_pipeline` khong co data nen Spark khong suy ra schema. "
                            "UI da uu tien dung `data/processed/features/clustering_fe`; neu van loi, hay chay lai preprocessing pipeline de rebuild feature artifacts."
                        ) from pipeline_exc
                    raise
    else:
        raw_df = raw_df_override
        feature_df, feature_warning = _build_uploaded_features(raw_df)

    combined_warning = upload_warning
    if feature_warning:
        combined_warning = (
            f"{combined_warning} {feature_warning}".strip()
            if combined_warning
            else feature_warning
        )

    try:
        pred_df = loaded.model.transform(feature_df)
        cols = [
            "customer_unique_id",
            "recency_days",
            "frequency_orders",
            "monetary_value",
            "prediction",
        ]
        selected_df = pred_df.select(*[c for c in cols if c in pred_df.columns])
        return _result_from_prediction_df(
            selected_df=selected_df,
            metadata=metadata,
            warning=combined_warning,
        )
    except Exception as exc:  # noqa: BLE001
        msg = str(exc)
        if (
            raw_df_override is None
            and model_key == "kmeans"
            and (
                "Python worker exited unexpectedly" in msg
                or "EOFException" in msg
                or "PythonRDD.runJob" in msg
            )
        ):
            logger.warning("Spark worker crash. Fallback offline KMeans.")
            return _run_kmeans_offline_prediction(metadata)
        raise


def run_clustering_prediction_from_upload(
    model_label: str,
    selected_k: int,
    max_iter: int,
    seed: int,
    uploaded_pdf: pd.DataFrame,
) -> ClusterResult:
    raw_df, warning = _prepare_uploaded_raw_df(uploaded_pdf)
    return run_clustering_prediction(
        model_label=model_label,
        selected_k=selected_k,
        max_iter=max_iter,
        seed=seed,
        raw_df_override=raw_df,
        upload_warning=warning,
    )


def export_cluster_csv(predictions_pdf: pd.DataFrame) -> bytes:
    return predictions_pdf.to_csv(index=False).encode("utf-8")
