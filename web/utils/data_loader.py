from __future__ import annotations

from pathlib import Path
from typing import Sequence

import pandas as pd
import streamlit as st
from pyspark.sql import DataFrame, functions as F

from utils.config import expected_data_keys, resolve_dataset_path
from utils.logger import get_logger
from utils.spark_helper import get_spark

logger = get_logger("data_loader")


class DataLoader:
    """Unified data loading utilities for parquet artifacts."""

    @staticmethod
    def _split_fallback_paths(dataset_key: str) -> list[Path]:
        if dataset_key != "rfm_customer_features":
            return []
        base_dir = resolve_dataset_path(dataset_key).parent
        candidates = [
            base_dir / "clustering_base_train",
            base_dir / "clustering_base_val",
            base_dir / "clustering_base_test",
        ]
        return [path for path in candidates if path.exists()]

    @staticmethod
    def dataset_path(dataset_key: str) -> Path:
        return resolve_dataset_path(dataset_key)

    @staticmethod
    def dataset_exists(dataset_key: str) -> bool:
        path = DataLoader.dataset_path(dataset_key)
        return path.exists() or bool(DataLoader._split_fallback_paths(dataset_key))

    @staticmethod
    def load_spark_df(dataset_key: str, required: bool = True) -> DataFrame:
        path = DataLoader.dataset_path(dataset_key)
        if not path.exists():
            fallback_paths = DataLoader._split_fallback_paths(dataset_key)
            if fallback_paths:
                logger.info(
                    "Doc du lieu parquet tu split fallback cho %s: %s",
                    dataset_key,
                    ", ".join(str(item) for item in fallback_paths),
                )
                spark = get_spark()
                return spark.read.parquet(*[str(item) for item in fallback_paths])
            msg = f"Khong tim thay dataset '{dataset_key}' tai: {path}"
            logger.error(msg)
            if required:
                raise FileNotFoundError(msg)
            spark = get_spark()
            return spark.createDataFrame([], schema="id string")
        logger.info("Doc du lieu parquet: %s", path)
        spark = get_spark()
        return spark.read.parquet(str(path))

    @staticmethod
    @st.cache_data(show_spinner=False)
    def load_pandas_sample(
        dataset_key: str,
        limit: int = 200,
        columns: Sequence[str] | None = None,
    ) -> pd.DataFrame:
        df = DataLoader.load_spark_df(dataset_key)
        if columns:
            keep_cols = [col for col in columns if col in df.columns]
            if keep_cols:
                df = df.select(*keep_cols)
        sample_df = df.limit(limit)
        try:
            return sample_df.toPandas()
        except Exception:
            try:
                rows = list(sample_df._jdf.toJSON().collect())
                if not rows:
                    return pd.DataFrame()
                import json

                return pd.DataFrame([json.loads(r) for r in rows])
            except Exception:
                return pd.DataFrame()

    @staticmethod
    @st.cache_data(show_spinner=False)
    def list_distinct_values(
        dataset_key: str, column: str, limit: int = 500
    ) -> list[str]:
        df = DataLoader.load_spark_df(dataset_key)
        if column not in df.columns:
            return []
        rows = (
            df.select(column)
            .where(F.col(column).isNotNull())
            .dropDuplicates([column])
            .orderBy(column)
            .limit(limit)
            .collect()
        )
        return [str(row[column]) for row in rows]

    @staticmethod
    @st.cache_data(show_spinner=False)
    def min_max_date(
        dataset_key: str, timestamp_col: str
    ) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
        df = DataLoader.load_spark_df(dataset_key)
        if timestamp_col not in df.columns:
            return None, None
        result = df.select(
            F.to_date(F.min(timestamp_col)).alias("min_d"),
            F.to_date(F.max(timestamp_col)).alias("max_d"),
        ).first()
        if result is None:
            return None, None
        return pd.to_datetime(result["min_d"]), pd.to_datetime(result["max_d"])

    @staticmethod
    @st.cache_data(show_spinner=False)
    def check_required_datasets() -> pd.DataFrame:
        rows: list[dict[str, str]] = []
        for key in expected_data_keys():
            path = DataLoader.dataset_path(key)
            fallback_paths = DataLoader._split_fallback_paths(key)
            exists = path.exists() or bool(fallback_paths)
            rows.append(
                {
                    "dataset": key,
                    "path": (
                        str(path)
                        if path.exists() or not fallback_paths
                        else " + ".join(str(item) for item in fallback_paths)
                    ),
                    "status": "San sang" if exists else "Thieu",
                }
            )
        return pd.DataFrame(rows)
