from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from services.eda_service import (
    apply_filters,
    correlation_matrix,
    delivery_performance,
    filtered_table,
    load_eda_base,
    load_filter_options,
    new_vs_returning,
    order_revenue_trend,
    payment_analysis,
    product_performance,
    review_distribution,
    revenue_by_category,
)
from utils.components import (
    apply_theme,
    dataframe_download_button,
    empty_state,
    error_state,
    section_header,
)
from utils.palette import PALETTE_DISCRETE

st.set_page_config(page_title="Xu hướng và Phân tích", page_icon="📈", layout="wide")
apply_theme()

st.title("Xu hướng và Phân tích")
st.caption(
    "Phân tích nâng cao theo thời gian, khách hàng, sản phẩm, review, vận chuyển và thanh toán."
)

try:
    options = load_filter_options()
except Exception as exc:  # noqa: BLE001
    error_state("Không tải được dữ liệu EDA.", str(exc))
    st.stop()

min_date, max_date = options.get("min_date"), options.get("max_date")
if min_date is None or max_date is None:
    error_state("Dataset EDA thiếu cột thời gian hợp lệ.")
    st.stop()

with st.sidebar:
    st.subheader("Bộ lọc")
    selected_dates = st.date_input(
        "Khoảng thời gian",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    quick_select = st.selectbox(
        "Chọn nhanh", ["All time", "7 ngày", "30 ngày", "90 ngày"]
    )
    if quick_select != "All time":
        day_count = int(quick_select.split()[0])
        end_date = pd.to_datetime(max_date)
        start_date = (end_date - pd.Timedelta(days=day_count)).date()
        selected_dates = (start_date, max_date)

    selected_categories = st.multiselect(
        "Category", options.get("categories", []), default=[]
    )
    selected_states = st.multiselect(
        "State/Region", options.get("states", []), default=[]
    )
    min_price = float(options.get("min_price", 0.0))
    max_price = float(options.get("max_price", 1000.0))
    selected_price = st.slider(
        "Price range (order_gmv)", min_price, max_price, (min_price, max_price)
    )
    granularity = st.radio("Granularity", ["Ngay", "Tuan", "Thang"], horizontal=True)

if not isinstance(selected_dates, tuple) or len(selected_dates) != 2:
    error_state("Khoảng thời gian không hợp lệ.")
    st.stop()

try:
    with st.spinner("Đang xử lý dữ liệu EDA..."):
        base_df = load_eda_base()
        filtered_df = apply_filters(
            base_df,
            selected_dates[0],
            selected_dates[1],
            selected_categories,
            selected_states,
            selected_price,
        )
        if filtered_df.limit(1).count() == 0:
            empty_state("Không có dữ liệu sau khi lọc.")
            st.stop()
except Exception as exc:  # noqa: BLE001
    error_state("Lỗi khi lọc dữ liệu EDA.", str(exc))
    st.stop()

section_header("Order trend over time")
trend_pdf = order_revenue_trend(filtered_df, granularity)
if trend_pdf.empty:
    empty_state("Không có dữ liệu xu hướng theo thời gian.")
else:
    left, right = st.columns(2)
    with left:
        fig_orders = px.line(
            trend_pdf,
            x="period",
            y="orders",
            markers=True,
            title="Số đơn theo thời gian",
        )
        st.plotly_chart(fig_orders, use_container_width=True)
    with right:
        fig_rev = px.line(
            trend_pdf,
            x="period",
            y="revenue",
            markers=True,
            title="Doanh thu theo thời gian",
        )
        st.plotly_chart(fig_rev, use_container_width=True)

section_header("Revenue analysis")
rev_cat_pdf = revenue_by_category(filtered_df, granularity)
if rev_cat_pdf.empty:
    empty_state("Không có dữ liệu revenue theo category.")
else:
    fig_stack = px.area(
        rev_cat_pdf,
        x="period",
        y="revenue",
        color="category",
        title="Stacked area doanh thu theo category",
    )
    st.plotly_chart(fig_stack, use_container_width=True)

section_header("Customer analysis")
cust_pdf = new_vs_returning(filtered_df, granularity)
if cust_pdf.empty:
    empty_state("Không đủ dữ liệu new/returning.")
else:
    fig_cust = px.area(
        cust_pdf, x="period", y="count", color="customer_type", title="New vs Returning"
    )
    st.plotly_chart(fig_cust, use_container_width=True)

section_header("Product performance")
top_products, worst_products = product_performance(filtered_df)
left, right = st.columns(2)
with left:
    if top_products.empty:
        empty_state("Không có dữ liệu top sellers.")
    else:
        fig_top = px.bar(
            top_products,
            x="order_count",
            y="category",
            orientation="h",
            title="Top sellers theo category",
            color_discrete_sequence=PALETTE_DISCRETE,
        )
        st.plotly_chart(fig_top, use_container_width=True)
with right:
    if worst_products.empty:
        empty_state("Không có dữ liệu worst sellers.")
    else:
        fig_worst = px.bar(
            worst_products,
            x="order_count",
            y="category",
            orientation="h",
            title="Worst sellers theo category",
            color_discrete_sequence=PALETTE_DISCRETE,
        )
        st.plotly_chart(fig_worst, use_container_width=True)

hist_pdf = (
    filtered_df.select("order_gmv")
    .where("order_gmv is not null")
    .limit(30000)
    .toPandas()
)
if not hist_pdf.empty:
    fig_hist = px.histogram(
        hist_pdf, x="order_gmv", nbins=50, title="Phân bố giá trị đơn hàng"
    )
    st.plotly_chart(fig_hist, use_container_width=True)

section_header("Review score distribution")
score_pdf, avg_score_pdf = review_distribution(filtered_df, granularity)
left, right = st.columns(2)
with left:
    if score_pdf.empty:
        empty_state("Không có phân bố review_score.")
    else:
        fig_score = px.bar(
            score_pdf,
            x="review_score",
            y="count",
            title="Review score 1-5",
            color_discrete_sequence=PALETTE_DISCRETE,
        )
        st.plotly_chart(fig_score, use_container_width=True)
with right:
    if avg_score_pdf.empty:
        empty_state("Không có review trung bình theo thời gian.")
    else:
        fig_avg_score = px.line(
            avg_score_pdf,
            x="period",
            y="avg_review_score",
            markers=True,
            title="Review trung bình theo thời gian",
        )
        st.plotly_chart(fig_avg_score, use_container_width=True)

section_header("Delivery performance")
delivery_pdf = delivery_performance(filtered_df)
left, right = st.columns(2)
with left:
    if delivery_pdf.empty:
        empty_state("Không có dữ liệu delivery_days.")
    else:
        fig_del_hist = px.histogram(
            delivery_pdf, x="delivery_days", nbins=40, title="Histogram delivery time"
        )
        st.plotly_chart(fig_del_hist, use_container_width=True)
with right:
    if delivery_pdf.empty:
        empty_state("Không có dữ liệu boxplot delivery.")
    else:
        fig_del_box = px.box(
            delivery_pdf,
            x="customer_state",
            y="delivery_days",
            title="Delivery by state",
        )
        st.plotly_chart(fig_del_box, use_container_width=True)

section_header("Payment analysis")
payment_trend_pdf, installments_pdf = payment_analysis(filtered_df, granularity)
left, right = st.columns(2)
with left:
    if payment_trend_pdf.empty:
        empty_state("Không có dữ liệu payment type theo thời gian.")
    else:
        fig_pay_stack = px.bar(
            payment_trend_pdf,
            x="period",
            y="count",
            color="payment_type",
            title="Payment type over time",
            color_discrete_sequence=PALETTE_DISCRETE,
        )
        st.plotly_chart(fig_pay_stack, use_container_width=True)
with right:
    if installments_pdf.empty:
        empty_state("Không có dữ liệu installments.")
    else:
        fig_install = px.bar(
            installments_pdf,
            x="payment_installments",
            y="count",
            title="Installments distribution",
            color_discrete_sequence=PALETTE_DISCRETE,
        )
        st.plotly_chart(fig_install, use_container_width=True)

section_header("Bảng dữ liệu sau lọc")
table_pdf = filtered_table(filtered_df)
search_text = st.text_input("Tìm theo order_id hoặc customer_unique_id")
if search_text.strip() and not table_pdf.empty:
    keyword = search_text.strip()
    mask = table_pdf["order_id"].astype(str).str.contains(
        keyword, case=False
    ) | table_pdf["customer_unique_id"].astype(str).str.contains(keyword, case=False)
    table_pdf = table_pdf[mask]

display_columns = st.multiselect(
    "Chọn cột hiển thị",
    options=table_pdf.columns.tolist(),
    default=table_pdf.columns.tolist(),
)
if display_columns:
    st.dataframe(table_pdf[display_columns], use_container_width=True, hide_index=True)
else:
    empty_state("Bạn cần chọn ít nhất 1 cột để hiển thị.")

left, right = st.columns(2)
with left:
    dataframe_download_button(table_pdf, "📥 Tải filtered CSV", "eda_filtered_data")
with right:
    current_view = table_pdf[display_columns] if display_columns else pd.DataFrame()
    dataframe_download_button(
        current_view, "📥 Tải current view CSV", "eda_current_view"
    )

if st.checkbox("Hiển thị correlation matrix (optional)", value=False):
    corr_df = correlation_matrix(table_pdf)
    if corr_df.empty:
        empty_state("Không đủ cột số để tính tương quan.")
    else:
        fig_corr = px.imshow(corr_df, text_auto=".2f", title="Correlation matrix")
        st.plotly_chart(fig_corr, use_container_width=True)
