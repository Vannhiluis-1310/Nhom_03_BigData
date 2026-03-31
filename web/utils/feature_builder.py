from __future__ import annotations

import json
from typing import Any

import pandas as pd
import streamlit as st

from utils.config import feature_schemas, resolve_dataset_path, resolve_pipeline_path
from utils.data_loader import DataLoader
from utils.logger import get_logger
from utils.spark_helper import get_spark

logger = get_logger("feature_builder")


def _task_base_dataset(task: str) -> str:
    if task == "classification":
        return "classification_inference_base"
    return "regression_inference_base"


def _assembler_required_columns(task: str) -> list[str]:
    """Read VectorAssembler input columns from FE pipeline metadata."""
    pipeline_path = resolve_pipeline_path(f"{task}_fe_pipeline")
    if not pipeline_path.exists():
        return []
    stages_dir = pipeline_path / "stages"
    if not stages_dir.exists():
        return []

    vector_stage = None
    for stage in sorted(stages_dir.iterdir()):
        if "VectorAssembler" in stage.name:
            vector_stage = stage
            break

    if vector_stage is None:
        return []

    meta_part = vector_stage / "metadata" / "part-00000"
    if not meta_part.exists():
        return []

    try:
        payload = json.loads(meta_part.read_text(encoding="utf-8"))
        input_cols = payload.get("paramMap", {}).get("inputCols", [])
    except Exception:  # noqa: BLE001
        return []

    required: list[str] = []
    for col_name in input_cols:
        if col_name == "customer_state_ohe":
            required.append("customer_state")
        elif col_name in (
            "text_features",
            "customer_state_idx",
            "features",
            "scaled_features",
        ):
            # Skip intermediate columns created by pipeline stages
            continue
        else:
            required.append(col_name)

    deduped: list[str] = []
    for item in required:
        if item not in deduped:
            deduped.append(item)
    return deduped


@st.cache_data(show_spinner=False)
def load_feature_schema(task: str) -> dict[str, Any]:
    schemas = feature_schemas()
    if task in schemas:
        return schemas[task]
    return {"fields": {}}


@st.cache_data(show_spinner=False)
def load_default_values(task: str) -> dict[str, Any]:
    dataset_key = _task_base_dataset(task)
    if not DataLoader.dataset_exists(dataset_key):
        return {}

    try:
        sample = DataLoader.load_pandas_sample(dataset_key=dataset_key, limit=500)
    except Exception:  # noqa: BLE001
        return {}
    if sample.empty:
        return {}

    defaults: dict[str, Any] = {}
    for col in sample.columns:
        series = sample[col].dropna()
        if series.empty:
            continue
        if pd.api.types.is_numeric_dtype(series):
            defaults[col] = float(series.median())
        else:
            defaults[col] = str(series.iloc[0])
    return defaults


def _coerce_value(dtype: str, value: Any, fallback: Any) -> Any:
    src = fallback if value is None or value == "" else value
    try:
        if dtype == "int":
            return int(float(src))
        if dtype == "float":
            return float(src)
        if dtype == "str":
            return str(src)
        return src
    except Exception:  # noqa: BLE001
        return fallback


def build_single_row_input(task: str, user_inputs: dict[str, Any]) -> dict[str, Any]:
    """Build single-row inference payload with strict schema order."""
    schema = load_feature_schema(task)
    fields = schema.get("fields", {})
    try:
        base_defaults = load_default_values(task)
    except Exception:  # noqa: BLE001
        base_defaults = {}
    required_columns = _assembler_required_columns(task)

    payload: dict[str, Any] = {
        "order_id": "streamlit_order_demo",
        "customer_unique_id": user_inputs.get(
            "customer_unique_id", "streamlit_customer_demo"
        ),
    }

    ordered_fields = list(fields.keys())
    for col_name in required_columns:
        if col_name not in ordered_fields:
            ordered_fields.append(col_name)

    for col_name in ordered_fields:
        spec = fields.get(col_name, {})
        dtype = str(spec.get("dtype", "str"))

        # Default values for numeric columns not in schema
        if col_name not in fields:
            if col_name in ("order_gmv", "payment_total_value", "order_freight_value"):
                dtype = "float"
            elif col_name in (
                "items_per_order",
                "unique_products_per_order",
                "unique_categories_per_order",
                "max_payment_installments",
            ):
                dtype = "int"

        default_val = spec.get(
            "default", base_defaults.get(col_name, "" if dtype == "str" else 0)
        )
        payload[col_name] = _coerce_value(dtype, user_inputs.get(col_name), default_val)

    if task == "regression" and "order_freight_value" not in payload:
        payload["order_freight_value"] = float(
            user_inputs.get("order_freight_value", 18.0)
        )

    # Add text fields required by pipeline but not in feature schema
    text_fields_defaults = {
        "review_text": "",
        "product_category_name": "",
        "product_name": "",
    }
    for text_col, default_val in text_fields_defaults.items():
        if text_col not in payload:
            payload[text_col] = default_val

    logger.info("Build input cho task=%s voi %d truong", task, len(payload))
    return payload


def build_single_row_spark_df(task: str, user_inputs: dict[str, Any]):
    row = build_single_row_input(task, user_inputs)
    spark = get_spark()

    def _sql_literal(value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            if pd.isna(value):
                return "NULL"
            return str(value)
        text = str(value).replace("'", "''")
        return f"'{text}'"

    cols = list(row.keys())
    values_sql = ", ".join(_sql_literal(row[c]) for c in cols)
    cols_sql = ", ".join([f"`{c}`" for c in cols])
    return spark.sql(f"SELECT * FROM VALUES ({values_sql}) AS t({cols_sql})")


@st.cache_data(show_spinner=False)
def load_form_options(task: str) -> dict[str, list[Any]]:
    """Get dropdown options from inference base datasets."""
    dataset_key = _task_base_dataset(task)
    path = resolve_dataset_path(dataset_key)
    options: dict[str, list[Any]] = {}
    if not path.exists():
        return options

    sample = DataLoader.load_pandas_sample(dataset_key=dataset_key, limit=2000)
    if "customer_state" in sample.columns:
        options["customer_state"] = sorted(
            sample["customer_state"].dropna().astype(str).unique().tolist()
        )
    return options
