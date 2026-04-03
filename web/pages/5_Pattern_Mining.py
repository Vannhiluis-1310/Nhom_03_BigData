from __future__ import annotations

import plotly.express as px
import streamlit as st

from services.pattern_mining_service import get_pattern_outputs
from utils.components import (
    apply_theme,
    dataframe_download_button,
    empty_state,
    error_state,
    section_header,
)
from utils.palette import PALETTE_CONTINUOUS

st.set_page_config(page_title="Khai phá luật kết hợp", page_icon="🛍️", layout="wide")
apply_theme()

st.title("Khai phá luật kết hợp")
st.caption(
    "Phân tích frequent itemsets và association rules từ kết quả FP-Growth đã lưu."
)

with st.sidebar:
    st.subheader("Thiết lập mining")
    mining_level = st.radio("Mining level", ["Theo Category", "Theo Product ID"])
    min_support = st.slider(
        "minSupport", min_value=0.001, max_value=0.50, value=0.001, step=0.001
    )
    min_confidence = st.slider(
        "minConfidence", min_value=0.05, max_value=0.99, value=0.10, step=0.01
    )
    min_itemsets = st.slider("minItemsets", min_value=1, max_value=6, value=2)
    top_rules = st.slider("Top rules", min_value=5, max_value=100, value=30)

try:
    with st.spinner("Đang tải output pattern mining..."):
        result = get_pattern_outputs(
            mining_level, min_support, min_confidence, min_itemsets, top_rules
        )
except FileNotFoundError as exc:
    error_state("Thiếu output frequent itemsets/rules.", str(exc))
    st.stop()
except Exception as exc:  # noqa: BLE001
    error_state("Lỗi khi xử lý pattern mining.", str(exc))
    st.stop()

if result.warning:
    st.warning(result.warning)

section_header("Frequent Itemsets")
if result.itemsets_pdf.empty:
    empty_state("Không có itemset phù hợp theo threshold hiện tại.")
else:
    search_kw = st.text_input("Tìm itemset", value="")
    itemsets_view = result.itemsets_pdf.copy()
    if search_kw.strip():
        mask = itemsets_view["itemsets_text"].str.contains(
            search_kw.strip(), case=False, na=False
        )
        itemsets_view = itemsets_view[mask]

    st.dataframe(
        itemsets_view[["itemsets_text", "support", "frequency", "item_count"]],
        use_container_width=True,
        hide_index=True,
    )

    chart_data = itemsets_view.head(20).sort_values("support")
    fig_itemsets = px.bar(
        chart_data,
        x="support",
        y="itemsets_text",
        orientation="h",
        title="Top 20 itemsets theo support",
        color="support",
        color_continuous_scale=PALETTE_CONTINUOUS,
    )
    st.plotly_chart(fig_itemsets, use_container_width=True)

section_header("Association Rules")
if result.rules_pdf.empty:
    empty_state("Không có luật phù hợp theo threshold hiện tại.")
else:
    st.dataframe(
        result.rules_pdf[["rule_text", "support", "confidence", "lift"]],
        use_container_width=True,
        hide_index=True,
    )
    fig_rules = px.scatter(
        result.rules_pdf,
        x="confidence",
        y="lift",
        size="support",
        color="lift",
        hover_data=["rule_text"],
        title="Rule Map: Confidence vs Lift",
        color_continuous_scale=PALETTE_CONTINUOUS,
    )
    st.plotly_chart(fig_rules, use_container_width=True)

section_header("Insights và gợi ý kinh doanh")
for insight in result.insights:
    st.write(f"- {insight}")

if result.metrics:
    st.caption(
        " | ".join(
            [
                f"transactions={result.metrics.get('transactions', '-')}",
                f"freq_itemsets={result.metrics.get('freq_itemsets', '-')}",
                f"rules={result.metrics.get('rules', '-')}",
            ]
        )
    )

left, right = st.columns(2)
with left:
    dataframe_download_button(result.rules_pdf, "📥 Tải rules CSV", "association_rules")
with right:
    dataframe_download_button(
        result.itemsets_pdf, "📥 Tải itemsets CSV", "frequent_itemsets"
    )
