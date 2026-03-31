from __future__ import annotations

import plotly.express as px
import streamlit as st

from services.clustering_service import run_clustering_prediction
from services.dashboard_service import (
    apply_filters,
    build_order_status,
    build_payment_distribution,
    build_revenue_trend,
    build_state_distribution,
    build_top_categories,
    compute_kpis,
    load_dashboard_base,
    load_filter_options,
    recent_orders,
)
from utils.components import (
    apply_theme,
    empty_state,
    error_state,
    kpi_card,
    section_header,
)
from utils.formatters import format_currency_brl, format_number, format_review_score
from utils.palette import PALETTE_CONTINUOUS, PALETTE_DISCRETE

st.set_page_config(
    page_title="Dashboard - Tổng quan hệ thống", page_icon="📊", layout="wide"
)
apply_theme()

st.title("Dashboard")
st.caption("Theo dõi KPI và xu hướng vận hành từ dữ liệu đơn hàng Olist.")

try:
    options = load_filter_options()
except Exception as exc:  # noqa: BLE001
    error_state("Không tải được dữ liệu dashboard.", str(exc))
    st.stop()

min_date = options.get("min_date")
max_date = options.get("max_date")
if min_date is None or max_date is None:
    error_state("Dữ liệu dashboard không có mốc thời gian hợp lệ.")
    st.stop()

with st.sidebar:
    st.subheader("Bộ lọc dữ liệu")
    selected_dates = st.date_input(
        "Khoảng ngày mua hàng",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    selected_states = st.multiselect("Bang", options.get("states", []), default=[])
    selected_cities = st.multiselect("Thành phố", options.get("cities", []), default=[])
    selected_payments = st.multiselect(
        "Phương thức thanh toán",
        options.get("payment_types", []),
        default=options.get("payment_types", []),
    )
    if st.button("🔄 Làm mới dữ liệu", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

if not isinstance(selected_dates, tuple) or len(selected_dates) != 2:
    error_state("Khoảng ngày chưa hợp lệ. Vui lòng chọn lại.")
    st.stop()

try:
    with st.spinner("Đang tải và lọc dữ liệu dashboard..."):
        base_df = load_dashboard_base()
        filtered_df = apply_filters(
            base_df,
            start_date=selected_dates[0],
            end_date=selected_dates[1],
            states=selected_states,
            cities=selected_cities,
            payment_types=selected_payments,
        )
        if filtered_df.limit(1).count() == 0:
            empty_state("Không có dữ liệu sau khi lọc. Bạn hãy nới điều kiện lọc.")
            st.stop()

        kpis = compute_kpis(filtered_df)
except Exception as exc:  # noqa: BLE001
    error_state("Lỗi khi xử lý dashboard.", str(exc))
    st.stop()

col1, col2, col3, col4 = st.columns(4)
with col1:
    kpi_card("Total Orders", format_number(kpis["total_orders"]))
with col2:
    kpi_card("Total Revenue", format_currency_brl(kpis["total_revenue"]))
with col3:
    kpi_card("Total Customers", format_number(kpis["total_customers"]))
with col4:
    kpi_card("Average Review Score", format_review_score(kpis["avg_review_score"]))

section_header("Xu hướng doanh thu và số đơn")
trend_pdf = build_revenue_trend(filtered_df)
if trend_pdf.empty:
    empty_state("Không có dữ liệu xu hướng doanh thu.")
else:
    left, right = st.columns(2)
    with left:
        fig_revenue = px.line(
            trend_pdf,
            x="month",
            y="doanh_thu",
            markers=True,
            title="Revenue Trend theo tháng",
            hover_data={"month": True, "doanh_thu": ":,.2f", "so_don": True},
        )
        fig_revenue.update_layout(xaxis_title="Tháng", yaxis_title="Doanh thu (R$)")
        st.plotly_chart(fig_revenue, use_container_width=True)
    with right:
        fig_orders = px.bar(
            trend_pdf,
            x="month",
            y="so_don",
            color="so_don",
            title="Order Volume theo tháng",
            color_continuous_scale=PALETTE_CONTINUOUS,
        )
        fig_orders.update_layout(xaxis_title="Tháng", yaxis_title="Số đơn")
        st.plotly_chart(fig_orders, use_container_width=True)

section_header("Cơ cấu danh mục và thanh toán")
cat_pdf = build_top_categories(filtered_df)
pay_pdf = build_payment_distribution(filtered_df)
left, right = st.columns(2)
with left:
    if cat_pdf.empty:
        empty_state("Không có dữ liệu category.")
    else:
        fig_cat = px.bar(
            cat_pdf.sort_values("count"),
            x="count",
            y="category",
            orientation="h",
            title="Top 10 danh mục",
            color="count",
            color_continuous_scale=PALETTE_CONTINUOUS,
        )
        fig_cat.update_layout(xaxis_title="Số đơn", yaxis_title="Danh mục")
        st.plotly_chart(fig_cat, use_container_width=True)
with right:
    if pay_pdf.empty:
        empty_state("Không có dữ liệu phương thức thanh toán.")
    else:
        fig_pay = px.pie(
            pay_pdf,
            names="payment_type",
            values="count",
            hole=0.45,
            title="Payment Methods",
            color_discrete_sequence=PALETTE_DISCRETE,
        )
        st.plotly_chart(fig_pay, use_container_width=True)

section_header("Trạng thái đơn hàng và địa lý")
status_pdf = build_order_status(filtered_df)
geo_pdf = build_state_distribution(filtered_df)
left, right = st.columns(2)
with left:
    if status_pdf.empty:
        empty_state("Không có dữ liệu trạng thái đơn.")
    else:
        # Define color mapping for order statuses - using provided pink/red palette
        status_colors = {
            "delivered": "#FFDBE7",  # Lightest pink
            "created": "#FFDAE9",  # Soft pink
            "approved": "#FF92B1",  # Light pink
            "invoiced": "#FD94B4",  # Medium pink
            "processing": "#F896B3",  # Pink-coral
            "shipped": "#F34455",  # Coral red
            "unavailable": "#EC1D3C",  # Bright red
            "canceled": "#DD2436",  # Deep red
        }

        # Define text colors for each status (pink tiles use dark text, red tiles use white)
        text_colors = {
            "delivered": "#2B0013",  # Black for light pink
            "created": "#2B0013",  # Black for light pink
            "approved": "#2B0013",  # Black for light pink
            "invoiced": "#2B0013",  # Black for medium pink
            "processing": "#2B0013",  # Black for pink-coral
            "shipped": "#FFFFFF",  # White for coral red
            "unavailable": "#FFFFFF",  # White for bright red
            "canceled": "#FFFFFF",  # White for deep red
        }

        # Add color columns
        status_pdf["color"] = status_pdf["order_status"].map(
            lambda x: status_colors.get(x.lower(), "#9ca3af")
        )
        status_pdf["text_color"] = status_pdf["order_status"].map(
            lambda x: text_colors.get(x.lower(), "#FFFFFF")
        )

        # Calculate percentage for display
        total = status_pdf["count"].sum()
        status_pdf["percentage"] = (status_pdf["count"] / total * 100).round(1)

        # Sort by count descending to match treemap order
        status_pdf = status_pdf.sort_values("count", ascending=False).reset_index(
            drop=True
        )

        # Build color mapping for plotly
        color_map = dict(zip(status_pdf["order_status"], status_pdf["color"]))

        # Create treemap with customdata for per-cell text styling
        fig_status = px.treemap(
            status_pdf,
            path=["order_status"],
            values="count",
            color="order_status",
            color_discrete_map=color_map,
            title="Order Status",
            custom_data=["percentage"],
        )

        # Ensure treemap uses our sort order (descending by count)
        fig_status.update_traces(sort=False)

        # Update treemap appearance with per-cell text colors
        # Get text colors in the same order as treemap cells
        text_color_list = status_pdf["text_color"].tolist()

        fig_status.update_traces(
            texttemplate="<b>%{label}</b><br>%{value:,}<br>(%{customdata[0]}%)",
            textposition="middle center",
            textfont=dict(
                size=14,
                color=text_color_list,
            ),
            hovertemplate="<b>%{label}</b><br>Số đơn: %{value:,}<br>Tỷ lệ: %{customdata[0]}%<extra></extra>",
        )

        fig_status.update_layout(
            margin=dict(t=30, b=10, l=10, r=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
        )

        st.plotly_chart(fig_status, use_container_width=True)
with right:
    if geo_pdf.empty:
        empty_state("Không có dữ liệu theo bang.")
    else:
        fig_geo = px.bar(
            geo_pdf.head(20),
            x="customer_state",
            y="count",
            color="count",
            color_continuous_scale=PALETTE_CONTINUOUS,
            title="Phân bố khách hàng theo bang",
        )
        fig_geo.update_layout(xaxis_title="Bang", yaxis_title="Số khách/đơn")
        st.plotly_chart(fig_geo, use_container_width=True)

section_header("Tổng quan phân khúc khách hàng")
try:
    cluster_result = run_clustering_prediction("KMeans", 6, 50, 42)
    if cluster_result.summary_pdf.empty:
        empty_state("Chưa có dữ liệu phân khúc để hiển thị.")
    else:
        c_left, c_right = st.columns(2)
        with c_left:
            cluster_dist = cluster_result.summary_pdf[
                ["cluster", "so_khach_hang"]
            ].rename(columns={"so_khach_hang": "count"})
            fig_cluster = px.pie(
                cluster_dist,
                names="cluster",
                values="count",
                hole=0.45,
                title="Tỷ trọng khách hàng theo cụm (KMeans)",
                color_discrete_sequence=PALETTE_DISCRETE,
            )
            st.plotly_chart(fig_cluster, use_container_width=True)
        with c_right:
            st.dataframe(
                cluster_result.summary_pdf,
                use_container_width=True,
                hide_index=True,
            )
except Exception as exc:  # noqa: BLE001
    st.caption(f"Không tải được chart clustering trên dashboard: {exc}")

section_header("5 đơn hàng gần nhất")
st.dataframe(recent_orders(filtered_df), use_container_width=True, hide_index=True)
