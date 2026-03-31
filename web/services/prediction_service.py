from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from pyspark.sql import functions as F

from utils.config import resolve_pipeline_path
from utils.feature_builder import (
    build_single_row_input,
    build_single_row_spark_df,
    load_feature_schema,
    load_form_options,
)
from utils.logger import get_logger
from utils.model_loader import (
    LoadedModel,
    load_classification_model,
    load_feature_pipeline,
    load_regression_model,
)

logger = get_logger("prediction_service")


CLASSIFICATION_MODELS = [
    "DecisionTree",
    "RandomForest",
    "LogisticRegression",
    "GBTClassifier",
    "NaiveBayes",
    "LinearSVC",
]

REGRESSION_MODELS = [
    "LinearRegression",
    "DecisionTreeRegressor",
    "RandomForestRegressor",
    "GBTRegressor",
]

SUPPORTED_TARGETS = {
    "classification": ["Dự đoán nguy cơ review thấp (<=3)"],
    "regression": ["Dự đoán order_value"],
}


@dataclass
class PredictionResult:
    prediction_value: float
    prediction_label: str
    probabilities: list[float] | None
    confidence: float | None
    decision_score: float | None
    metadata: dict[str, Any]
    raw_output: pd.DataFrame


def _parse_vector(values: Any) -> list[float] | None:
    if values is None:
        return None
    if hasattr(values, "toArray"):
        return [float(x) for x in values.toArray().tolist()]
    if isinstance(values, (list, tuple)):
        return [float(x) for x in values]
    return None


def _label_for_classification(prediction_value: float) -> str:
    return (
        "Nguy co review thap (<=3)"
        if int(round(prediction_value)) == 1
        else "Review cao (>3)"
    )


def _load_model(task: str, model_name: str) -> LoadedModel:
    if task == "classification":
        return load_classification_model(model_name)
    return load_regression_model(model_name)


def _extract_feature_importance(model_obj: Any, top_n: int = 10) -> pd.DataFrame:
    if not hasattr(model_obj, "featureImportances"):
        return pd.DataFrame(columns=["feature", "importance"])
    try:
        arr = model_obj.featureImportances.toArray().tolist()
    except Exception:  # noqa: BLE001
        return pd.DataFrame(columns=["feature", "importance"])

    imp_df = pd.DataFrame(
        {
            "feature": [f"Feature_{i}" for i in range(len(arr))],
            "importance": arr,
        }
    )
    return imp_df.sort_values("importance", ascending=False).head(top_n)


def _read_vector_assembler_inputs(task: str) -> list[str]:
    pipeline_path = resolve_pipeline_path(f"{task}_fe_pipeline")
    stages_dir = pipeline_path / "stages"
    if not stages_dir.exists():
        return []
    for stage in sorted(stages_dir.iterdir()):
        if "VectorAssembler" not in stage.name:
            continue
        meta_file = stage / "metadata" / "part-00000"
        if not meta_file.exists():
            continue
        try:
            payload = json.loads(meta_file.read_text(encoding="utf-8"))
            return payload.get("paramMap", {}).get("inputCols", [])
        except Exception:  # noqa: BLE001
            return []
    return []


def get_feature_importance(task: str, model_name: str) -> pd.DataFrame:
    loaded = _load_model(task, model_name)
    imp_df = _extract_feature_importance(loaded.model)
    if imp_df.empty:
        return imp_df

    assembler_cols = _read_vector_assembler_inputs(task)
    rename_map: dict[str, str] = {}
    for idx, col_name in enumerate(assembler_cols):
        rename_map[f"Feature_{idx}"] = col_name
    imp_df["feature"] = imp_df["feature"].map(lambda x: rename_map.get(x, x))
    return imp_df


def predict(
    task: str, model_name: str, user_inputs: dict[str, Any]
) -> PredictionResult:
    loaded_model = _load_model(task, model_name)
    fe_pipeline = load_feature_pipeline(task)

    input_sdf = build_single_row_spark_df(task, user_inputs)
    transformed = fe_pipeline.transform(input_sdf)
    pred_df = loaded_model.model.transform(transformed)

    select_cols = ["prediction"]
    for optional_col in ["probability", "rawPrediction"]:
        if optional_col in pred_df.columns:
            select_cols.append(optional_col)

    output_rows = pred_df.select(*select_cols).limit(1).collect()
    if not output_rows:
        raise RuntimeError("Khong nhan duoc ket qua prediction tu Spark model.")
    first_row = output_rows[0].asDict(recursive=True)
    output = pd.DataFrame([first_row])
    pred_value = float(first_row["prediction"])

    probs = (
        _parse_vector(first_row["probability"])
        if "probability" in output.columns
        else None
    )
    raw_pred = (
        _parse_vector(first_row["rawPrediction"])
        if "rawPrediction" in output.columns
        else None
    )

    confidence = max(probs) if probs else None
    decision_score = None
    if probs is None and raw_pred:
        if len(raw_pred) == 2:
            decision_score = float(raw_pred[1] - raw_pred[0])
        else:
            decision_score = float(raw_pred[0])

    if task == "classification":
        label = _label_for_classification(pred_value)
    else:
        label = "Gia tri du doan"

    return PredictionResult(
        prediction_value=pred_value,
        prediction_label=label,
        probabilities=probs,
        confidence=confidence,
        decision_score=decision_score,
        metadata=loaded_model.metadata,
        raw_output=output,
    )


def build_form_schema(task: str) -> dict[str, Any]:
    schema = load_feature_schema(task)
    options = load_form_options(task)
    return {"schema": schema, "options": options}


def build_payload(task: str, user_inputs: dict[str, Any]) -> dict[str, Any]:
    return build_single_row_input(task, user_inputs)
