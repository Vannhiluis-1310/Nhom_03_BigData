from __future__ import annotations

from datetime import datetime

import streamlit as st

from services.admin_service import system_status
from utils.components import (
    apply_theme,
    feature_card,
    hero_section,
    metric_row,
    section_header,
    status_badge,
)

st.set_page_config(
    page_title="Olist Analytics Report Center", page_icon="📊", layout="wide"
)
apply_theme()

hero_section(
    title="Trung tâm báo cáo và mô hình Olist",
    subtitle=(
        "Không gian làm việc tổng hợp cho dashboard điều hành, phân tích xu hướng, "
        "machine learning inference, recommendation và quản trị hệ thống trên Streamlit + PySpark."
    ),
    badges=[
        "Executive dashboard",
        "PySpark pipelines",
        "ML serving",
        "Recommendation",
    ],
)

with st.spinner("Đang kiểm tra trạng thái hệ thống..."):
    status = system_status()

spark_status = "Đã kết nối" if status["spark_ok"] else "Mất kết nối"
data_status = status["data_status"]
model_status = status["model_status"]

ready_data = (
    int((data_status["status"] == "San sang").sum()) if not data_status.empty else 0
)
ready_models = (
    int((model_status["status"] == "San sang").sum()) if not model_status.empty else 0
)

metrics = [
    {
        "title": "Spark Runtime",
        "value": spark_status,
        "caption": "Trạng thái kết nối hiện tại",
    },
    {
        "title": "Dataset sẵn sàng",
        "value": f"{ready_data}/{len(data_status)}",
        "caption": "Tài nguyên dữ liệu có thể sử dụng",
    },
    {
        "title": "Model sẵn sàng",
        "value": f"{ready_models}/{len(model_status)}",
        "caption": "Artifacts đã nạp thành công",
    },
    {
        "title": "Entry point",
        "value": "web/Home.py",
        "caption": "Khởi chạy bằng streamlit run web/Home.py",
    },
]

section_header(
    "Tổng quan hệ thống",
    "Theo dõi nhanh độ sẵn sàng của runtime, dataset và model phục vụ cho demo báo cáo.",
)

metric_row(metrics)

last_check = datetime.now().strftime("%d/%m/%Y %H:%M")

left_col, right_col = st.columns([1.2, 1])

with left_col:
    st.markdown(
        """
        <div class='olist-card'>
            <div class='olist-eyebrow'>Executive Summary</div>
            <div style='font-size: 1.15rem; font-weight: 700; color: var(--text-main); line-height: 1.5;'>
                Tập trung vào vận hành đơn hàng, hiệu suất kinh doanh và khả năng kích hoạt mô hình trong cùng một giao diện.
            </div>
            <p style='margin-top: 12px; color: var(--text-muted); line-height: 1.7; font-size: 0.9rem;'>
                Bộ giao diện đã được chuyển sang phong cách báo cáo chuyên nghiệp hơn: nền sáng, màu sắc trung tính,
                thông tin được nhóm theo module và ưu tiên tính đọc để theo dõi trên màn hình điều hành.
            </p>
            <ul class='olist-list'>
                <li>Dashboard và EDA phục vụ theo dõi KPI, doanh thu, thanh toán và xu hướng hành vi.</li>
                <li>Clustering, prediction và recommendation tạo lớp demo cho khả năng phân tích nâng cao.</li>
                <li>Pattern mining và admin hỗ trợ ra quyết định, vận hành pipeline và kiểm soát tài nguyên.</li>
            </ul>
        </div>
        """,
        unsafe_allow_html=True,
    )

with right_col:
    st.markdown(
        f"""
        <div class='olist-card'>
            <div class='olist-eyebrow'>Operational Readiness</div>
            <div style='font-size: 1.05rem; font-weight: 700; color: var(--text-main);'>Mức độ sẵn sàng hiện tại</div>
            <div style='margin-top: 14px;'>{status_badge("Spark đang hoạt động", status["spark_ok"])}</div>
            <div class='olist-info-grid' style='margin-top: 18px;'>
                <div class='olist-info-cell'>
                    <div class='olist-info-key'>Dữ liệu</div>
                    <div class='olist-info-value'>{ready_data}/{len(data_status)} dataset sẵn sàng</div>
                </div>
                <div class='olist-info-cell'>
                    <div class='olist-info-key'>Model</div>
                    <div class='olist-info-value'>{ready_models}/{len(model_status)} model có sẵn</div>
                </div>
                <div class='olist-info-cell'>
                    <div class='olist-info-key'>Cập nhật</div>
                    <div class='olist-info-value'>{last_check}</div>
                </div>
                <div class='olist-info-cell'>
                    <div class='olist-info-key'>Command</div>
                    <div class='olist-info-value'><code>streamlit run web/Home.py</code></div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

section_header(
    "Danh mục module",
    "Mỗi module được tổ chức như một workspace báo cáo hoặc mô hình riêng trong menu điều hướng bên trái.",
)

features = [
    (
        "Dashboard vận hành",
        "KPI tổng quan, xu hướng doanh thu, trạng thái đơn hàng và phân bố địa lý cho cấp quản lý.",
        "01",
        "Đơn hàng, doanh thu, thanh toán, địa lý",
    ),
    (
        "Clustering khách hàng",
        "Phân khúc RFM với các mô hình clustering để nhận diện nhóm khách hàng giá trị và hành vi.",
        "02",
        "Không gian làm việc cho segmentation",
    ),
    (
        "Recommendation",
        "Gợi ý sản phẩm theo user hoặc sản phẩm đang xem với ALS và cơ chế fallback theo đồng mua.",
        "03",
        "Cá nhân hóa và cross-sell",
    ),
    (
        "Prediction",
        "Dự đoán classification và regression từ bộ form đầu vào một dòng phục vụ demo mô hình.",
        "04",
        "Suy luận mô hình Spark ML",
    ),
    (
        "Pattern mining",
        "Khai phá frequent itemsets và association rules để tìm mô hình đồng mua và cơ hội bundle.",
        "05",
        "Phân tích giỏ hàng",
    ),
    (
        "EDA trends",
        "Theo dõi xu hướng theo thời gian, khách hàng, review, vận chuyển và thanh toán.",
        "06",
        "Khám phá dữ liệu và báo cáo xu hướng",
    ),
    (
        "Admin",
        "Quản lý upload dữ liệu, chạy pipeline, retrain, rollback và giám sát log hệ thống.",
        "07",
        "Vận hành và quản trị",
    ),
]

cols_per_row = 2
for i in range(0, len(features), cols_per_row):
    cols = st.columns(cols_per_row)
    for j, col in enumerate(cols):
        idx = i + j
        if idx < len(features):
            title, desc, code, footer = features[idx]
            with col:
                feature_card(title, desc, code, footer)

section_header(
    "Năng lực phân tích và mô hình",
    "Tóm tắt nhanh phạm vi bài toán và nhóm thuật toán được đưa vào ứng dụng.",
)

overview_left, overview_right = st.columns(2)

with overview_left:
    st.markdown(
        """
        <div class='olist-card'>
            <div class='olist-eyebrow'>Use Cases</div>
            <div style='font-size: 1.02rem; font-weight: 700; color: var(--text-main);'>Phạm vi nghiệp vụ</div>
            <ul class='olist-list'>
                <li>Quan sát doanh thu, đơn hàng, review và thanh toán theo góc nhìn điều hành.</li>
                <li>Đánh giá hành vi mua sắm, xu hướng danh mục và hiệu quả giao hàng theo thời gian.</li>
                <li>Hỗ trợ ra quyết định với segmentation, prediction, recommendation và basket analysis.</li>
            </ul>
        </div>
        """,
        unsafe_allow_html=True,
    )

with overview_right:
    st.markdown(
        """
        <div class='olist-card'>
            <div class='olist-eyebrow'>Model Stack</div>
            <div style='font-size: 1.02rem; font-weight: 700; color: var(--text-main);'>Nhóm thuật toán chính</div>
            <ul class='olist-list'>
                <li>Classification và regression: Logistic Regression, Random Forest, LinearSVC, GBT, Linear Regression.</li>
                <li>Clustering: KMeans, BisectingKMeans, GaussianMixture cho RFM segmentation.</li>
                <li>Recommendation và pattern mining: ALS collaborative filtering, FP-Growth, Plotly reporting.</li>
            </ul>
        </div>
        """,
        unsafe_allow_html=True,
    )

section_header(
    "Trạng thái chi tiết tài nguyên",
    "Bảng dữ liệu và artifacts model được trình bày tách riêng để dễ kiểm tra trước khi demo.",
)

tab_data, tab_model = st.tabs(["Dataset", "Model"])

with tab_data:
    st.dataframe(data_status, use_container_width=True, hide_index=True)

with tab_model:
    st.dataframe(model_status, use_container_width=True, hide_index=True)
