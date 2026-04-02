from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


def project_root() -> Path:
    """Return project root folder from web/utils/*."""
    return Path(__file__).resolve().parents[2]


ROOT = project_root()
WEB_DIR = ROOT / "web"
CONFIG_DIR = WEB_DIR / "config"
LOG_DIR = WEB_DIR / "logs"
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
MODELS_DIR = ROOT / "models"
REPORTS_DIR = ROOT / "reports"
NOTEBOOKS_DIR = ROOT / "notebooks"

MODEL_REGISTRY_PATH = CONFIG_DIR / "model_registry.yaml"
FEATURE_SCHEMA_PATH = CONFIG_DIR / "feature_schemas.yaml"


@lru_cache(maxsize=1)
def load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    return data


@lru_cache(maxsize=1)
def model_registry() -> dict[str, Any]:
    return load_yaml(MODEL_REGISTRY_PATH)


@lru_cache(maxsize=1)
def feature_schemas() -> dict[str, Any]:
    return load_yaml(FEATURE_SCHEMA_PATH)


def _resolve_relative(path_like: str | Path) -> Path:
    path = Path(path_like)
    if path.is_absolute():
        return path
    return ROOT / path


def resolve_dataset_path(dataset_key: str) -> Path:
    datasets = model_registry().get("datasets", {})
    rel = datasets.get(dataset_key, "")
    return _resolve_relative(rel) if rel else ROOT / f"data/processed/{dataset_key}"


def resolve_model_path(model_family: str, model_name: str) -> Path:
    models = model_registry().get("models", {})
    family_map = models.get(model_family, {})
    rel = family_map.get(model_name, "")
    if rel:
        return _resolve_relative(rel)
    return MODELS_DIR / model_family / model_name


def resolve_pipeline_path(pipeline_key: str) -> Path:
    pipelines = model_registry().get("pipelines", {})
    rel = pipelines.get(pipeline_key, "")
    return (
        _resolve_relative(rel)
        if rel
        else PROCESSED_DIR / "features" / "models" / pipeline_key
    )


def resolve_metric_path(model_family: str, model_name: str) -> Path:
    metrics = model_registry().get("metrics", {})
    family_map = metrics.get(model_family, {})
    rel = family_map.get(model_name, "")
    if rel:
        return _resolve_relative(rel)
    return REPORTS_DIR / "model_metrics" / f"{model_family}_{model_name}.json"


def resolve_script_list(script_group: str) -> list[Path]:
    scripts = model_registry().get("scripts", {})
    entries = scripts.get(script_group, [])
    if not isinstance(entries, list):
        return []
    return [_resolve_relative(item) for item in entries]


def resolve_retrain_script(model_family: str) -> Path | None:
    retrain = model_registry().get("scripts", {}).get("retrain", {})
    rel = retrain.get(model_family)
    if not rel:
        return None
    return _resolve_relative(rel)


def expected_data_keys() -> list[str]:
    return [
        "master_orders",
        "dashboard_base",
        "eda_base",
        "rfm_customer_features",
        "recommendation_interactions",
        "product_lookup",
        "customer_lookup",
        "classification_inference_base",
        "regression_inference_base",
        "pattern_baskets",
        "association_rules",
        "frequent_itemsets",
    ]


def expected_models() -> dict[str, list[str]]:
    return {
        "classification": [
            "logistic_regression",
            "random_forest_classifier",
            "naive_bayes",
            "linear_svc",
            "gbt_classifier",
            "decision_tree_classifier",
        ],
        "regression": [
            "linear_regression",
            "decision_tree_regressor",
            "random_forest_regressor",
        ],
        "clustering": ["kmeans", "bisecting_kmeans", "gaussian_mixture"],
        "recommendation": ["als"],
        "pattern_mining": ["fpgrowth"],
    }
