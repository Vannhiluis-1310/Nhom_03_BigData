from __future__ import annotations

import plotly.express as px
import streamlit as st

from services.prediction_service import (
    CLASSIFICATION_MODELS,
    REGRESSION_MODELS,
    SUPPORTED_TARGETS,
    build_form_schema,
    build_payload,
    get_feature_importance,
    predict,
)
from utils.components import (
    apply_theme,
    empty_state,
    error_state,
    model_info_card,
    section_header,
)
from utils.formatters import format_currency_brl, format_percent
from utils.palette import PALETTE_CONTINUOUS

st.set_page_config(page_title="Dự đoán", page_icon="🔮", layout="wide")
apply_theme()

st.title("Dự đoán")
st.caption("Dự đoán classification/regression từ model Spark MLlib đã train.")

with st.sidebar:
    task_mode = st.radio(
        "Chọn bài toán", ["Classification", "Regression"], horizontal=False
    )

task_key = "classification" if task_mode == "Classification" else "regression"
model_choices = (
    CLASSIFICATION_MODELS if task_mode == "Classification" else REGRESSION_MODELS
)
selected_model = st.selectbox("Chọn model", model_choices)

target_choice = st.selectbox("Target", SUPPORTED_TARGETS[task_key])

try:
    form_cfg = build_form_schema(task_key)
except Exception as exc:  # noqa: BLE001
    error_state("Không thể tải feature schema.", str(exc))
    st.stop()

schema_fields = form_cfg.get("schema", {}).get("fields", {})
options = form_cfg.get("options", {})

section_header("Nhập dữ liệu đầu vào")
user_inputs: dict[str, object] = {}
with st.form("prediction_form"):
    cols = st.columns(3)
    idx = 0
    for field_name, spec in schema_fields.items():
        dtype = str(spec.get("dtype", "str"))
        label = str(spec.get("label", field_name))
        default = spec.get("default")
        col = cols[idx % 3]

        with col:
            if field_name in options and options[field_name]:
                default_val = (
                    default
                    if default in options[field_name]
                    else options[field_name][0]
                )
                user_inputs[field_name] = st.selectbox(
                    label,
                    options[field_name],
                    index=options[field_name].index(default_val),
                )
            elif spec.get("options"):
                allowed = list(spec["options"])
                default_val = default if default in allowed else allowed[0]
                user_inputs[field_name] = st.selectbox(
                    label, allowed, index=allowed.index(default_val)
                )
            elif dtype == "int":
                user_inputs[field_name] = st.number_input(
                    label,
                    min_value=int(spec.get("min", 0)),
                    max_value=int(spec.get("max", 1000000)),
                    value=int(default or 0),
                    step=1,
                )
            elif dtype == "float":
                user_inputs[field_name] = st.number_input(
                    label,
                    min_value=float(spec.get("min", 0.0)),
                    value=float(default or 0.0),
                )
            else:
                user_inputs[field_name] = st.text_input(label, value=str(default or ""))
        idx += 1

    submitted = st.form_submit_button("🚀 Dự đoán", use_container_width=True)

if not submitted:
    st.info("Nhập thông tin và bấm 'Dự đoán' để xem kết quả.")
    st.stop()

try:
    with st.spinner("Đang chạy inference..."):
        payload = build_payload(task_key, user_inputs)
        result = predict(task_key, selected_model, payload)
except FileNotFoundError as exc:
    error_state("Thiếu artifact model hoặc pipeline.", str(exc))
    st.stop()
except Exception as exc:  # noqa: BLE001
    error_state("Lỗi khi dự đoán.", str(exc))
    st.stop()

section_header("Kết quả dự đoán")
if task_mode == "Classification":
    st.success(
        f"Nhãn dự đoán: **{result.prediction_label}** (class={int(result.prediction_value)})"
    )
    if result.probabilities:
        prob_df = {
            "class": [str(i) for i in range(len(result.probabilities))],
            "probability": result.probabilities,
        }
        fig_prob = px.bar(
            prob_df,
            x="class",
            y="probability",
            color="probability",
            color_continuous_scale=PALETTE_CONTINUOUS,
        )
        fig_prob.update_layout(
            title="Xác suất theo lớp", xaxis_title="Class", yaxis_title="Probability"
        )
        st.plotly_chart(fig_prob, use_container_width=True)
        st.caption(f"Confidence: {format_percent(result.confidence)}")
    elif result.decision_score is not None:
        st.info(
            "Model này không hỗ trợ probability chuẩn (ví dụ LinearSVC). "
            f"Decision score: {result.decision_score:.4f}"
        )
    else:
        empty_state("Model không trả về probability/decision score.")
else:
    value = result.prediction_value
    st.success(f"Giá trị dự đoán: **{format_currency_brl(value)}**")
    rmse = result.metadata.get("rmse") or result.metadata.get("test_rmse")
    if rmse is not None:
        low = value - float(rmse)
        high = value + float(rmse)
        st.caption(
            f"Khoảng dự đoán tham khảo (±RMSE): {format_currency_brl(low)} đến {format_currency_brl(high)}"
        )
    else:
        st.info("Model hiện tại chưa hỗ trợ prediction interval chính thức.")

section_header("Feature Importance")
imp_df = get_feature_importance(task_key, selected_model)
if imp_df.empty:
    empty_state(
        "Model này không hỗ trợ feature importance (hoặc không phải tree-based)."
    )
else:
    fig_imp = px.bar(
        imp_df.sort_values("importance"),
        x="importance",
        y="feature",
        orientation="h",
        title="Top 10 feature quan trọng",
        color="importance",
        color_continuous_scale=PALETTE_CONTINUOUS,
    )
    st.plotly_chart(fig_imp, use_container_width=True)

section_header("Thông tin model")
meta = result.metadata
if task_mode == "Classification":
    model_info_card(
        {
            "Model": selected_model,
            "Accuracy": meta.get("accuracy", meta.get("test_accuracy", "-")),
            "Precision": meta.get("precision", "-"),
            "Recall": meta.get("recall", "-"),
            "F1": meta.get("f1", meta.get("test_f1", "-")),
            "Train rows": meta.get("train_rows", "-"),
        }
    )
else:
    model_info_card(
        {
            "Model": selected_model,
            "RMSE": meta.get("rmse", meta.get("test_rmse", "-")),
            "MAE": meta.get("mae", meta.get("test_mae", "-")),
            "R²": meta.get("r2", meta.get("test_r2", "-")),
            "Train rows": meta.get("train_rows", "-"),
        }
    )
