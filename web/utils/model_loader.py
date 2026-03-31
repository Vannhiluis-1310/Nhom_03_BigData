from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st
from pyspark.ml import PipelineModel
from pyspark.ml.classification import (
    DecisionTreeClassificationModel,
    GBTClassificationModel,
    LinearSVCModel,
    LogisticRegressionModel,
    NaiveBayesModel,
    RandomForestClassificationModel,
)
from pyspark.ml.clustering import (
    BisectingKMeansModel,
    GaussianMixtureModel,
    KMeansModel,
)
from pyspark.ml.recommendation import ALSModel
from pyspark.ml.regression import (
    DecisionTreeRegressionModel,
    GBTRegressionModel,
    LinearRegressionModel,
    RandomForestRegressionModel,
)

from utils.config import (
    expected_models,
    resolve_dataset_path,
    resolve_metric_path,
    resolve_model_path,
    resolve_pipeline_path,
)
from utils.logger import get_logger
from utils.spark_helper import get_spark

logger = get_logger("model_loader")


@dataclass
class LoadedModel:
    name: str
    family: str
    model: Any
    path: Path
    metadata: dict[str, Any]


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {}


def _has_spark_metadata(path: Path) -> bool:
    return (path / "metadata" / "part-00000").exists()


def _resolve_artifact_path(model_path: Path) -> Path:
    if not model_path.exists():
        return model_path
    if _has_spark_metadata(model_path):
        return model_path

    candidates = [d for d in model_path.iterdir() if d.is_dir()]
    if not candidates:
        return model_path

    candidates = sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)
    for candidate in candidates:
        if _has_spark_metadata(candidate):
            logger.info("Dung artifact version moi nhat: %s", candidate)
            return candidate
    return model_path


def _read_spark_artifact_metadata(model_path: Path) -> dict[str, Any]:
    metadata_part = model_path / "metadata" / "part-00000"
    if not metadata_part.exists():
        return {}
    try:
        payload = metadata_part.read_text(encoding="utf-8")
        return json.loads(payload)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Khong doc duoc metadata Spark tai %s: %s", metadata_part, exc)
        return {}


def load_model_metadata(
    model_path: Path, model_family: str = "", model_name: str = ""
) -> dict[str, Any]:
    """Load metadata from model folder and metrics registry."""
    combined: dict[str, Any] = {}

    for filename in [
        "metadata.json",
        "training_info.json",
        "metrics.json",
        "label_info.json",
    ]:
        combined.update(_read_json(model_path / filename))

    combined.update(_read_spark_artifact_metadata(model_path))

    if model_family and model_name:
        combined.update(_read_json(resolve_metric_path(model_family, model_name)))

    combined["artifact_path"] = str(model_path)
    return combined


def _load_with_fallback(model_path: Path, loader_classes: list[type]) -> Any:
    artifact_path = _resolve_artifact_path(model_path)
    if not artifact_path.exists():
        raise FileNotFoundError(f"Khong tim thay model artifact tai: {model_path}")
    get_spark()
    for cls in loader_classes:
        try:
            return cls.load(str(artifact_path))
        except Exception:  # noqa: BLE001
            continue
    raise RuntimeError(f"Khong the load artifact tai {artifact_path}")


def _normalize_classification_name(model_name: str) -> str:
    mapping = {
        "LogisticRegression": "logistic_regression",
        "RandomForest": "random_forest_classifier",
        "RandomForestClassifier": "random_forest_classifier",
        "NaiveBayes": "naive_bayes",
        "LinearSVC": "linear_svc",
        "GBTClassifier": "gbt_classifier",
        "DecisionTree": "decision_tree_classifier",
        "DecisionTreeClassifier": "decision_tree_classifier",
    }
    return mapping.get(model_name, model_name)


def _normalize_regression_name(model_name: str) -> str:
    mapping = {
        "LinearRegression": "linear_regression",
        "DecisionTreeRegressor": "decision_tree_regressor",
        "DecisionTree": "decision_tree_regressor",
        "RandomForestRegressor": "random_forest_regressor",
        "RandomForest": "random_forest_regressor",
        "GBTRegressor": "gbt_regressor",
    }
    return mapping.get(model_name, model_name)


def _normalize_clustering_name(model_name: str) -> str:
    mapping = {
        "KMeans": "kmeans",
        "BisectingKMeans": "bisecting_kmeans",
        "GaussianMixture": "gaussian_mixture",
    }
    return mapping.get(model_name, model_name)


def _clustering_loader_classes(model_key: str) -> list[type]:
    mapping: dict[str, list[type]] = {
        "kmeans": [KMeansModel],
        "bisecting_kmeans": [BisectingKMeansModel],
        "gaussian_mixture": [GaussianMixtureModel],
    }
    return mapping.get(model_key, [PipelineModel])


@st.cache_resource(show_spinner=False)
def load_classification_model(model_name: str) -> LoadedModel:
    model_key = _normalize_classification_name(model_name)
    configured_path = resolve_model_path("classification", model_key)
    model_path = _resolve_artifact_path(configured_path)
    logger.info("Load classification model: %s", model_path)
    model = _load_with_fallback(
        configured_path,
        [
            LogisticRegressionModel,
            RandomForestClassificationModel,
            NaiveBayesModel,
            LinearSVCModel,
            GBTClassificationModel,
            DecisionTreeClassificationModel,
            PipelineModel,
        ],
    )
    meta = load_model_metadata(model_path, "classification", model_key)
    return LoadedModel(model_key, "classification", model, model_path, meta)


@st.cache_resource(show_spinner=False)
def load_regression_model(model_name: str) -> LoadedModel:
    model_key = _normalize_regression_name(model_name)
    configured_path = resolve_model_path("regression", model_key)
    model_path = _resolve_artifact_path(configured_path)
    logger.info("Load regression model: %s", model_path)
    model = _load_with_fallback(
        configured_path,
        [
            LinearRegressionModel,
            DecisionTreeRegressionModel,
            RandomForestRegressionModel,
            GBTRegressionModel,
            PipelineModel,
        ],
    )
    meta = load_model_metadata(model_path, "regression", model_key)
    return LoadedModel(model_key, "regression", model, model_path, meta)


@st.cache_resource(show_spinner=False)
def load_clustering_model(model_name: str) -> LoadedModel:
    model_key = _normalize_clustering_name(model_name)
    configured_path = resolve_model_path("clustering", model_key)
    model_path = _resolve_artifact_path(configured_path)
    logger.info("Load clustering model: %s", model_path)
    model = _load_with_fallback(configured_path, _clustering_loader_classes(model_key))
    meta = load_model_metadata(model_path, "clustering", model_key)
    return LoadedModel(model_key, "clustering", model, model_path, meta)


@st.cache_resource(show_spinner=False)
def load_recommendation_model() -> LoadedModel:
    model_key = "als"
    configured_path = resolve_model_path("recommendation", model_key)
    model_path = _resolve_artifact_path(configured_path)
    logger.info("Load recommendation model: %s", model_path)
    model = _load_with_fallback(configured_path, [ALSModel])
    meta = load_model_metadata(model_path, "recommendation", model_key)
    return LoadedModel(model_key, "recommendation", model, model_path, meta)


@st.cache_resource(show_spinner=False)
def load_pattern_mining_artifacts() -> dict[str, Any]:
    """Load frequent itemsets and association rules from saved outputs."""
    spark = get_spark()
    itemsets_path = resolve_dataset_path("frequent_itemsets")
    rules_path = resolve_dataset_path("association_rules")

    if not itemsets_path.exists() or not rules_path.exists():
        raise FileNotFoundError(
            "Thieu output pattern mining. Can co frequent_itemsets va association_rules."
        )

    itemsets_df = spark.read.parquet(str(itemsets_path))
    rules_df = spark.read.parquet(str(rules_path))
    metrics = _read_json(resolve_metric_path("pattern_mining", "fpgrowth"))
    return {
        "itemsets_df": itemsets_df,
        "rules_df": rules_df,
        "metrics": metrics,
        "itemsets_path": str(itemsets_path),
        "rules_path": str(rules_path),
    }


@st.cache_resource(show_spinner=False)
def load_feature_pipeline(task: str) -> PipelineModel:
    key = f"{task}_fe_pipeline"
    pipeline_path = resolve_pipeline_path(key)
    if not pipeline_path.exists():
        raise FileNotFoundError(f"Khong tim thay FE pipeline: {pipeline_path}")
    return PipelineModel.load(str(pipeline_path))


@st.cache_data(show_spinner=False)
def list_model_availability() -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    expected = expected_models()
    for family, names in expected.items():
        for name in names:
            configured_path = resolve_model_path(family, name)
            path = _resolve_artifact_path(configured_path)
            rows.append(
                {
                    "family": family,
                    "model": name,
                    "path": str(path),
                    "status": "San sang" if _has_spark_metadata(path) else "Thieu",
                }
            )
    return pd.DataFrame(rows)
