from __future__ import annotations

import streamlit as st

from services.admin_service import (
    compare_model_metrics,
    clear_streamlit_cache,
    list_family_versions,
    load_activity_log,
    load_error_log,
    rollback_family_version,
    run_reporting_pipeline,
    run_preprocessing_pipeline,
    run_retrain_pipeline,
    save_uploaded_files,
    scan_models,
    system_status,
    validate_uploaded_file,
)
from utils.components import (
    apply_theme,
    empty_state,
    error_state,
    model_info_card,
    section_header,
)
from utils.spark_helper import python_runtime_info

st.set_page_config(page_title="Admin - Quản trị hệ thống", page_icon="⚙️", layout="wide")
apply_theme()

st.title("Admin - Quản trị hệ thống")
st.caption(
    "Quản lý data/model artifacts, chạy pipeline và theo dõi trạng thái hệ thống."
)

section_header("1) Data Management")
st.info(
    "Lưu ý: tải file lên chỉ lưu vào `data/raw`. Hệ thống chỉ tạo lại `train/val/test` "
    "khi bạn chạy `Chạy preprocessing pipeline`. Bước `Retrain model` sẽ dùng các tập split "
    "đang có sẵn trong `data/processed`, không tự tách lại từ file vừa upload. "
    "Nếu muốn dùng file custom để demo, hãy sửa `config/raw_input_mapping.yaml` trước khi chạy preprocessing."
)
uploads = st.file_uploader(
    "Tải file CSV/Parquet lên `data/raw`",
    type=["csv", "parquet"],
    accept_multiple_files=True,
)

valid_files = []
if uploads:
    for up in uploads:
        result = validate_uploaded_file(up)
        with st.expander(
            f"{up.name} | {'✅ Hợp lệ' if result.valid else '❌ Lỗi'}", expanded=False
        ):
            st.write(f"Kết quả: {result.message}")
            st.write(f"Số dòng preview: {result.rows}")
            st.write(f"Lược đồ cột: {result.columns}")
            if not result.preview.empty:
                st.dataframe(result.preview, use_container_width=True)
        if result.valid:
            valid_files.append(up)

if st.button(
    "💾 Lưu file vào data/raw", use_container_width=True, disabled=not valid_files
):
    saved = save_uploaded_files(valid_files)
    st.success(f"Đã lưu {len(saved)} file vào data/raw")
    st.toast("Upload thành công")

st.markdown("**Pipeline steps**")
st.markdown(
    """
    1. Đọc dữ liệu raw từ `data/raw`
    2. Làm sạch và chuẩn hóa dữ liệu
    3. Join bảng và build silver/gold
    4. Tạo lại các tập `train/val/test`
    5. Tạo feature artifacts và lưu parquet
    """
)
st.caption(
    "Nếu bạn upload file mới hoặc file đặc biệt, hãy chạy preprocessing trước rồi mới retrain "
    "để metrics `train/val/test` phản ánh đúng dữ liệu hiện tại. "
    "Pipeline sẽ ưu tiên đọc mapping trong `config/raw_input_mapping.yaml`."
)

if st.button("▶️ Chạy preprocessing pipeline", use_container_width=True):
    progress = st.progress(0)
    progress_value = 0
    log_container = st.empty()
    logs: list[str] = []
    failed = False
    for line in run_preprocessing_pipeline():
        logs.append(line)
        log_container.text_area("Pipeline log", "\n".join(logs[-250:]), height=260)
        if "[EXIT_CODE] 0" in line:
            progress_value = min(100, progress_value + 25)
            progress.progress(progress_value)
        elif "[ERROR]" in line or "[EXIT_CODE] 1" in line:
            progress.progress(100)
            st.error("Pipeline thất bại. Kiểm tra log bên trên.")
            failed = True
            break
    if not failed:
        progress.progress(100)
        st.success("Pipeline kết thúc thành công.")
        st.toast("Preprocessing hoàn tất")

if st.button("🧾 Tạo báo cáo metrics tự động", use_container_width=True):
    report_logs: list[str] = []
    report_box = st.empty()
    report_failed = False
    for line in run_reporting_pipeline():
        report_logs.append(line)
        report_box.text_area("Reporting log", "\n".join(report_logs[-250:]), height=220)
        if "[ERROR]" in line or "[EXIT_CODE] 1" in line:
            st.error("Tạo báo cáo thất bại. Kiểm tra log.")
            report_failed = True
            break
    if not report_failed:
        st.success("Đã tạo báo cáo metrics tự động.")
        st.toast("Report generation hoàn tất")

section_header("2) Model Management")
family = st.selectbox(
    "Loại model",
    ["classification", "regression", "clustering", "recommendation", "pattern_mining"],
)

model_df = scan_models()
family_df = model_df[model_df["family"] == family] if not model_df.empty else model_df
if family_df.empty:
    empty_state("Chưa có model nào trong nhóm này.")
else:
    selected_model = st.selectbox("Model hiện tại", family_df["model"].tolist())
    current_row = family_df[family_df["model"] == selected_model].head(1)
    if not current_row.empty:
        row = current_row.iloc[0]
        model_info_card(
            {
                "Tên model": row["model"],
                "Phiên bản": "current",
                "Thời gian cập nhật": row["last_modified"],
                "Đường dẫn": row["path"],
                "Metrics": row["metrics"],
            },
            title="Thông tin model hiện tại",
        )

use_new_only = st.checkbox("Chỉ dùng dữ liệu mới", value=False)
tune_hyper = st.checkbox("Tinh chỉnh siêu tham số", value=False)
if st.button("🔁 Retrain model", use_container_width=True):
    st.info(
        f"Cấu hình retrain: family={family}, new_only={use_new_only}, tune={tune_hyper}. "
        "Retrain sẽ dùng các tập split hiện có trong `data/processed`."
    )
    progress = st.progress(0)
    progress_value = 0
    log_box = st.empty()
    logs: list[str] = []
    retrain_failed = False
    for line in run_retrain_pipeline(family):
        logs.append(line)
        log_box.text_area("Retrain log", "\n".join(logs[-250:]), height=260)
        if "[EXIT_CODE] 0" in line:
            progress.progress(100)
            st.success("Retrain thành công")
            st.toast("Retrain hoàn tất")
        elif "[ERROR]" in line or "[EXIT_CODE] 1" in line:
            progress.progress(100)
            st.error("Retrain thất bại. Kiểm tra log.")
            retrain_failed = True
            break
        elif "[INFO]" in line:
            progress_value = min(95, progress_value + 10)
            progress.progress(progress_value)
    if not retrain_failed:
        st.caption("Retrain hoàn tất và đã lưu snapshot version.")

section_header("Quản lý phiên bản model")
if family_df.empty:
    empty_state("Không có version để hiển thị.")
else:
    st.dataframe(
        family_df[["model", "last_modified", "path"]],
        use_container_width=True,
        hide_index=True,
    )
    metrics_compare_df = compare_model_metrics(family)
    if metrics_compare_df.empty:
        st.caption("Chưa có metrics JSON để so sánh.")
    else:
        st.markdown("**So sánh metrics**")
        st.dataframe(metrics_compare_df, use_container_width=True, hide_index=True)
    version_df = list_family_versions(family)
    if version_df.empty:
        st.caption("Chưa có snapshot version.")
    else:
        st.markdown("**Các snapshot version**")
        st.dataframe(version_df, use_container_width=True, hide_index=True)
        rollback_choice = st.selectbox(
            "Chọn version để rollback",
            options=version_df["version"].tolist(),
        )
        if st.button("📦 Rollback về version đã chọn"):
            try:
                rollback_family_version(family, rollback_choice)
                st.success(f"Rollback thành công về version: {rollback_choice}")
                st.toast("Rollback hoàn tất")
            except Exception as exc:  # noqa: BLE001
                st.error(f"Rollback thất bại: {exc}")

section_header("3) System Status")
try:
    status = system_status()
except Exception as exc:  # noqa: BLE001
    error_state("Không lấy được system status.", str(exc))
    st.stop()

c1, c2, c3 = st.columns(3)
with c1:
    st.metric("Spark connection", "Connected" if status["spark_ok"] else "Disconnected")
with c2:
    ready_data = (
        int((status["data_status"]["status"] == "San sang").sum())
        if not status["data_status"].empty
        else 0
    )
    st.metric("Data availability", f"{ready_data}/{len(status['data_status'])}")
with c3:
    ready_models = (
        int((status["model_status"]["status"] == "San sang").sum())
        if not status["model_status"].empty
        else 0
    )
    st.metric("Model availability", f"{ready_models}/{len(status['model_status'])}")

with st.expander("Activity log", expanded=False):
    activity_df = load_activity_log()
    if activity_df.empty:
        empty_state("Chưa có activity log.")
    else:
        st.dataframe(activity_df.tail(200), use_container_width=True, hide_index=True)

with st.expander("Error log", expanded=False):
    error_df = load_error_log()
    if error_df.empty:
        empty_state("Chưa có error log.")
    else:
        st.dataframe(error_df.tail(200), use_container_width=True, hide_index=True)

with st.expander("Python runtime debug", expanded=False):
    st.json(python_runtime_info())

section_header("4) Settings")
default_page = st.selectbox(
    "Default page",
    [
        "Home",
        "1_Dashboard",
        "2_Clustering",
        "3_Recommendation",
        "4_Prediction",
        "5_Pattern_Mining",
        "6_EDA_Trends",
        "7_Admin",
    ],
)
auto_refresh = st.checkbox("Auto refresh data", value=False)
st.caption(
    f"Cài đặt hiện tại: default_page={default_page}, auto_refresh={auto_refresh}"
)

btn_left, btn_right = st.columns(2)
with btn_left:
    if st.button("Clear cache", use_container_width=True):
        clear_streamlit_cache()
        st.success("Đã clear cache")
        st.toast("Cache đã xoá")
with btn_right:
    if st.button("Rebuild all", use_container_width=True):
        st.info(
            "Gợi ý: chạy preprocessing trước, sau đó retrain lần lượt từng family model."
        )
