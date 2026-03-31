from __future__ import annotations

import streamlit as st

from services.recommendation_service import (
    available_users,
    get_product_based_recommendations,
    get_user_history,
    get_user_recommendations,
    load_assets,
    random_user,
    search_products,
)
from utils.components import apply_theme, empty_state, error_state, section_header
from utils.formatters import format_currency_brl, format_percent

st.set_page_config(page_title="Gợi ý sản phẩm", page_icon="🛒", layout="wide")
apply_theme()

st.title("Gợi ý sản phẩm")
st.caption(
    "Hệ gợi ý dựa trên ALS collaborative filtering, có fallback theo sản phẩm đồng mua."
)

try:
    assets = load_assets()
except FileNotFoundError as exc:
    error_state("Thiếu model ALS hoặc dữ liệu recommendation.", str(exc))
    st.stop()
except Exception as exc:  # noqa: BLE001
    error_state("Không khởi tạo được dịch vụ recommendation.", str(exc))
    st.stop()

mode = st.radio(
    "Chọn chế độ", ["Theo User ID", "Theo sản phẩm đang xem"], horizontal=True
)
top_n = st.slider("Top-N", 5, 20, 10)

with st.expander("Thông tin phương pháp", expanded=False):
    param_map = (
        assets.metadata.get("paramMap", {}) if isinstance(assets.metadata, dict) else {}
    )
    st.write("Model: ALS (Alternating Least Squares)")
    st.write(f"Rank: {assets.metadata.get('rank', '-')}")
    st.write(f"RegParam: {param_map.get('regParam', '-')}")
    st.write(f"Số bản ghi train: {assets.metadata.get('train_rows', '-')}")
    if "rmse" in assets.metadata:
        st.write(f"RMSE: {assets.metadata.get('rmse')}")
    if "precision_at_k" in assets.metadata:
        st.write(
            f"Precision@K: {format_percent(float(assets.metadata.get('precision_at_k', 0.0)))}"
        )
    if "recall_at_k" in assets.metadata:
        st.write(
            f"Recall@K: {format_percent(float(assets.metadata.get('recall_at_k', 0.0)))}"
        )
    if "map_at_k" in assets.metadata:
        st.write(
            f"MAP@K: {format_percent(float(assets.metadata.get('map_at_k', 0.0)))}"
        )
    if "ndcg_at_k" in assets.metadata:
        st.write(
            f"NDCG@K: {format_percent(float(assets.metadata.get('ndcg_at_k', 0.0)))}"
        )

if mode == "Theo User ID":
    users = available_users(limit=300)
    user_input = st.text_input("Nhập customer_id hoặc customer_unique_id")
    picked_user = st.selectbox(
        "Hoặc chọn nhanh user", options=["(không chọn)"] + users
    )
    if st.button("User ngẫu nhiên"):
        random_pick = random_user()
        if random_pick:
            st.session_state["random_user_pick"] = random_pick

    if picked_user != "(không chọn)":
        user_input = picked_user
    if st.session_state.get("random_user_pick"):
        user_input = user_input or st.session_state["random_user_pick"]
        st.caption(f"User ngẫu nhiên: {st.session_state['random_user_pick']}")

    if st.button("Gợi ý cho user", use_container_width=True):
        rec_pdf, warning = get_user_recommendations(user_input, top_n)
        if warning:
            st.warning(warning)
            sample = random_user()
            if sample:
                st.info(f"Bạn có thể thử user mẫu: {sample}")
        elif rec_pdf.empty:
            empty_state("Không có gợi ý phù hợp cho user này.")
        else:
            section_header("Top sản phẩm gợi ý")
            if "avg_price" in rec_pdf.columns:
                rec_pdf["avg_price_fmt"] = rec_pdf["avg_price"].apply(
                    format_currency_brl
                )
            st.dataframe(rec_pdf, use_container_width=True, hide_index=True)

            section_header("Lịch sử mua hàng của user")
            history_pdf = get_user_history(user_input)
            if history_pdf.empty:
                empty_state("User chưa có lịch sử mua trong tập interactions.")
            else:
                st.dataframe(history_pdf, use_container_width=True, hide_index=True)
            st.info(
                "Users similar to you also bought: collaborative filtering dựa trên hành vi mua tương tự."
            )

else:
    product_input = st.text_input("Nhập product_id")
    search_kw = st.text_input("Tìm sản phẩm theo tên/category")
    matched = search_products(search_kw, limit=30)
    if not matched.empty:
        st.dataframe(matched, use_container_width=True, hide_index=True)
        picked = st.selectbox(
            "Chọn product_id nhanh",
            options=["(không chọn)"] + matched["product_id"].astype(str).tolist(),
        )
        if picked != "(không chọn)":
            product_input = picked

    if st.button("Gợi ý theo sản phẩm", use_container_width=True):
        rec_pdf, explain = get_product_based_recommendations(product_input, top_n)
        st.caption(explain)
        if rec_pdf.empty:
            empty_state("Không có gợi ý theo sản phẩm này.")
        else:
            st.dataframe(rec_pdf, use_container_width=True, hide_index=True)
