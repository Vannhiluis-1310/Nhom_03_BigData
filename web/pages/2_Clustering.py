from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from services.clustering_service import (
    export_cluster_csv,
    run_clustering_prediction,
    run_clustering_prediction_from_upload,
)
from utils.components import (
    apply_theme,
    empty_state,
    error_state,
    model_info_card,
    section_header,
)
from utils.palette import PALETTE_CONTINUOUS, PALETTE_DISCRETE

st.set_page_config(
    page_title="Phân khúc khách hàng",
    page_icon="👥",
    layout="wide",
)
apply_theme()

st.title("Phân khúc khách hàng")
st.caption(
    "Sử dụng model clustering đã train sẵn để gán cụm và phân tích hồ sơ khách hàng."
)

with st.sidebar:
    st.subheader("Cấu hình phân cụm")
    model_label = st.selectbox(
        "Chọn model", ["KMeans", "BisectingKMeans", "GaussianMixture"]
    )
    selected_k = st.slider("Số cụm k", 2, 10, 6)
    max_iter = st.slider("maxIter", 10, 200, 50)
    seed = st.number_input("Seed", min_value=0, value=42, step=1)
    uploaded_file = st.file_uploader(
        "Upload CSV để phân cụm",
        type=["csv"],
        help=(
            "CSV có thể gồm customer_unique_id để map theo dữ liệu hệ thống, "
            "hoặc đầy đủ cột customer_unique_id, recency_days, frequency_orders, monetary_value."
        ),
    )

try:
    with st.spinner("Đang tải model và dự đoán cụm..."):
        if uploaded_file is not None:
            uploaded_pdf = pd.read_csv(uploaded_file)
            result = run_clustering_prediction_from_upload(
                model_label,
                selected_k,
                int(max_iter),
                int(seed),
                uploaded_pdf,
            )
        else:
            result = run_clustering_prediction(
                model_label, selected_k, int(max_iter), int(seed)
            )
except FileNotFoundError as exc:
    error_state("Thiếu model clustering hoặc dữ liệu RFM.", str(exc))
    st.stop()
except ValueError as exc:
    error_state("File upload clustering chưa đúng định dạng.", str(exc))
    st.stop()
except Exception as exc:  # noqa: BLE001
    if "schema Parquet" in str(exc) or "nested parquet layout" in str(exc):
        error_state(
            "Artifact model clustering khong tuong thich voi UI hien tai.",
            str(exc),
        )
        st.stop()
    if "PYTHON_VERSION_MISMATCH" in str(exc):
        error_state(
            "Lỗi phiên bản Python giữa Spark driver và worker.",
            "Vui lòng Clear cache ở trang Admin và khởi động lại Streamlit để tạo lại SparkSession đồng bộ Python.",
        )
        st.stop()
    if "Python worker exited unexpectedly" in str(exc) or "EOFException" in str(exc):
        error_state(
            "Python worker của Spark bị crash.",
            "Hãy chạy app bằng Python 3.10/3.11, bấm Clear cache ở trang Admin, rồi chạy lại."
            " Trang này hiện đã tự giới hạn số bản ghi convert sang Pandas để giảm lỗi worker.",
        )
        st.stop()
    error_state("Lỗi khi chạy clustering.", str(exc))
    st.stop()

if result.warning:
    st.warning(result.warning)
    if result.predictions_pdf.empty:
        st.stop()

if result.predictions_pdf.empty:
    empty_state("Không có kết quả phân cụm để hiển thị.")
    st.stop()

if result.is_sampled:
    st.info(
        f"Hiển thị mẫu {len(result.predictions_pdf):,}/{result.total_rows:,} bản ghi để tăng ổn định. "
        "Bảng thống kê cụm vẫn tính trên toàn bộ dữ liệu."
    )

section_header("Phân bố cụm trên không gian RFM")
scatter_fig = px.scatter(
    result.predictions_pdf,
    x="recency_days",
    y="monetary_value",
    color="cluster",
    hover_data=["customer_unique_id", "frequency_orders"],
    title="Recency vs Monetary theo cụm",
)
scatter_fig.update_layout(xaxis_title="Recency (ngày)", yaxis_title="Monetary (R$)")
st.plotly_chart(scatter_fig, use_container_width=True)

left, right = st.columns(2)
with left:
    dist_pdf = result.summary_pdf[["cluster", "so_khach_hang"]].rename(
        columns={"so_khach_hang": "count"}
    )
    donut_fig = px.pie(
        dist_pdf,
        names="cluster",
        values="count",
        hole=0.5,
        title="Tỷ lệ khách hàng theo cụm",
        color_discrete_sequence=PALETTE_DISCRETE,
    )
    st.plotly_chart(donut_fig, use_container_width=True)

with right:
    st.markdown("#### Thống kê cụm")
    st.dataframe(result.summary_pdf, use_container_width=True, hide_index=True)

section_header("RFM Heatmap theo cụm")
heatmap_source = result.heatmap_pdf.set_index("cluster")[
    ["Recency", "Frequency", "Monetary"]
]
heatmap_fig = px.imshow(
    heatmap_source,
    text_auto=".2f",
    aspect="auto",
    color_continuous_scale=PALETTE_CONTINUOUS,
    title="Giá trị trung bình RFM theo cluster",
)
st.plotly_chart(heatmap_fig, use_container_width=True)

section_header("Hồ sơ cụm khách hàng")
for cluster_id, profile_text in result.profiles.items():
    with st.expander(f"Cluster {cluster_id} - {profile_text}"):
        row = result.summary_pdf[result.summary_pdf["cluster"] == cluster_id].head(1)
        if not row.empty:
            st.write(
                f"Số KH: {int(row['so_khach_hang'].iloc[0])} | "
                f"Avg Revenue: {row['avg_revenue'].iloc[0]:.2f} | "
                f"Avg Orders: {row['avg_orders'].iloc[0]:.2f} | "
                f"Avg Recency: {row['avg_recency'].iloc[0]:.2f}"
            )

model_info_card(
    {
        "Model": model_label,
        "k từ metadata": result.metadata.get("paramMap", {}).get(
            "k", result.metadata.get("k", "-")
        ),
        "Silhouette": result.metadata.get("silhouette", "-"),
        "Rows train": result.metadata.get("rows", "-"),
    },
    title="Thông tin model clustering",
)

st.download_button(
    "📥 Tải kết quả phân cụm CSV",
    data=export_cluster_csv(result.predictions_pdf),
    file_name="customer_clusters.csv",
    mime="text/csv",
)
