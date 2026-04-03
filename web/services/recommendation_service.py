from __future__ import annotations

import json
import random
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
import streamlit as st
from pyspark.sql import DataFrame, functions as F

from utils.data_loader import DataLoader
from utils.logger import get_logger
from utils.model_loader import load_recommendation_model
from utils.spark_helper import get_spark

logger = get_logger("recommendation_service")


def _to_pandas_safe(df: DataFrame, fail_silently: bool = True) -> pd.DataFrame:
    try:
        return df.toPandas()
    except Exception as ex:
        logger.warning(
            "toPandas failed, fallback to JSON collect: %s", type(ex).__name__
        )
        try:
            json_rows = list(df._jdf.toJSON().collect())
            if not json_rows:
                return pd.DataFrame()
            return pd.DataFrame([json.loads(row) for row in json_rows])
        except Exception as inner_ex:
            logger.exception(
                "JSON collect fallback failed: %s", type(inner_ex).__name__
            )
            if not fail_silently:
                raise RuntimeError(
                    f"Khong the collect Spark DataFrame sang pandas ({type(inner_ex).__name__})"
                ) from inner_ex
            return pd.DataFrame()


@dataclass
class RecommendationAssets:
    als_model: Any
    metadata: dict[str, Any]
    interactions_df: DataFrame
    products_df: DataFrame
    customer_lookup_df: DataFrame


@st.cache_resource(show_spinner="Đang tải dữ liệu recommendation...")
def load_assets() -> RecommendationAssets:
    loaded = load_recommendation_model()
    interactions = DataLoader.load_spark_df("recommendation_interactions")

    products = DataLoader.load_spark_df("product_lookup")
    if "product_name" not in products.columns:
        products = products.withColumn("product_name", F.col("product_id"))

    order_items = DataLoader.load_spark_df("order_items", required=False)
    if "product_id" in order_items.columns and "price" in order_items.columns:
        avg_price = order_items.groupBy("product_id").agg(
            F.round(F.avg("price"), 2).alias("avg_price")
        )
        products = products.join(avg_price, "product_id", "left")
    else:
        products = products.withColumn("avg_price", F.lit(None).cast("double"))

    customer_lookup = DataLoader.load_spark_df("customer_lookup")
    return RecommendationAssets(
        loaded.model, loaded.metadata, interactions, products, customer_lookup
    )


def available_users(limit: int = 500) -> list[str]:
    assets = load_assets()
    user_factors = assets.als_model.userFactors.select(
        F.col("id").cast("int").alias("user_idx")
    )
    eligible_users = (
        assets.interactions_df.select("customer_unique_id", "user_idx")
        .dropDuplicates(["customer_unique_id", "user_idx"])
        .join(user_factors, "user_idx", "inner")
    )
    return [
        row[0]
        for row in eligible_users.select("customer_unique_id")
        .dropDuplicates(["customer_unique_id"])
        .orderBy("customer_unique_id")
        .limit(limit)
        .collect()
    ]


def random_user() -> str | None:
    users = available_users(limit=1000)
    if not users:
        return None
    return random.choice(users)


def _map_to_customer_unique_id(raw_user_id: str) -> str | None:
    assets = load_assets()
    input_value = (raw_user_id or "").strip()
    if not input_value:
        return None

    match_unique = (
        assets.interactions_df.where(F.col("customer_unique_id") == input_value)
        .select("customer_unique_id")
        .limit(1)
        .collect()
    )
    if match_unique:
        return str(match_unique[0]["customer_unique_id"])

    match_customer = (
        assets.customer_lookup_df.where(F.col("customer_id") == input_value)
        .select("customer_unique_id")
        .limit(1)
        .collect()
    )
    if match_customer:
        return str(match_customer[0]["customer_unique_id"])
    return None


def _attach_product_metadata(
    df: DataFrame, score_col: str, fail_silently: bool = True
) -> pd.DataFrame:
    assets = load_assets()
    item_map = assets.interactions_df.select("item_idx", "product_id").dropDuplicates(
        ["item_idx"]
    )
    out = (
        df.join(item_map, "item_idx", "left")
        .join(assets.products_df, "product_id", "left")
        .orderBy(F.desc(score_col))
    )
    out = _to_pandas_safe(out, fail_silently=fail_silently)
    if "product_category_name" not in out.columns:
        out["product_category_name"] = "Khong ro"
    out["product_name"] = out.get("product_name", out.get("product_id", ""))
    return out


def _fallback_recommend_from_history(
    customer_unique_id: str, top_n: int, assets: RecommendationAssets
) -> pd.DataFrame:
    baskets = DataLoader.load_spark_df("pattern_baskets", required=False)
    if "items" not in baskets.columns:
        return pd.DataFrame()

    history_items = (
        assets.interactions_df.where(F.col("customer_unique_id") == customer_unique_id)
        .select("product_id")
        .where(F.col("product_id").isNotNull())
        .dropDuplicates(["product_id"])
    )
    seeds = history_items.select(F.col("product_id").alias("seed_product_id"))
    candidates = (
        baskets.select("items")
        .where(F.col("items").isNotNull())
        .join(
            seeds, F.array_contains(F.col("items"), F.col("seed_product_id")), "inner"
        )
        .select(F.explode("items").alias("product_id"))
        .join(history_items, "product_id", "left_anti")
        .groupBy("product_id")
        .count()
        .orderBy(F.desc("count"), F.asc("product_id"))
        .limit(top_n)
        .withColumnRenamed("count", "predicted_score")
    )
    return _to_pandas_safe(candidates.join(assets.products_df, "product_id", "left"))


def get_user_recommendations(
    raw_user_id: str, top_n: int
) -> tuple[pd.DataFrame, str | None]:
    assets = load_assets()
    customer_unique_id = _map_to_customer_unique_id(raw_user_id)
    if customer_unique_id is None:
        return pd.DataFrame(), "Khong tim thay user trong du lieu huan luyen."

    user_factors = assets.als_model.userFactors.select(
        F.col("id").cast("int").alias("user_idx")
    )
    user_idx_row = (
        assets.interactions_df.where(F.col("customer_unique_id") == customer_unique_id)
        .select("user_idx")
        .dropDuplicates(["user_idx"])
        .join(user_factors, "user_idx", "inner")
        .limit(1)
        .collect()
    )
    if not user_idx_row:
        fallback = _fallback_recommend_from_history(customer_unique_id, top_n, assets)
        if not fallback.empty:
            return fallback, None
        return pd.DataFrame(), "User khong co mapping user_idx hop le cho ALS."

    user_idx = int(user_idx_row[0]["user_idx"])
    user_subset = (
        assets.interactions_df.where(F.col("user_idx") == F.lit(user_idx))
        .select(F.col("user_idx").cast("int").alias("user_idx"))
        .dropDuplicates(["user_idx"])
        .limit(1)
    )
    rec = assets.als_model.recommendForUserSubset(user_subset, top_n)
    rec_flat = rec.select(F.explode("recommendations").alias("rec")).select(
        F.col("rec.item_idx").alias("item_idx"),
        F.col("rec.rating").alias("predicted_score"),
    )
    collect_error: str | None = None
    try:
        output = _attach_product_metadata(
            rec_flat, "predicted_score", fail_silently=False
        )
    except RuntimeError as ex:
        collect_error = str(ex)
        output = pd.DataFrame()

    if output.empty:
        fallback = _fallback_recommend_from_history(customer_unique_id, top_n, assets)
        if not fallback.empty:
            if collect_error:
                return (
                    fallback,
                    f"ALS collect gap loi ({collect_error}). Da fallback sang co-purchase.",
                )
            return fallback, None
        if collect_error:
            return (
                pd.DataFrame(),
                f"Khong xuat duoc goi y ALS do loi collect: {collect_error}",
            )
        return pd.DataFrame(), "Khong co goi y phu hop cho user nay."
    return output, None


def get_user_history(raw_user_id: str, limit: int = 50) -> pd.DataFrame:
    assets = load_assets()
    customer_unique_id = _map_to_customer_unique_id(raw_user_id)
    if customer_unique_id is None:
        return pd.DataFrame()

    history = (
        assets.interactions_df.where(F.col("customer_unique_id") == customer_unique_id)
        .join(assets.products_df, "product_id", "left")
        .select("product_id", "product_name", "product_category_name", "rating")
        .dropDuplicates(["product_id"])
        .limit(limit)
    )
    history = _to_pandas_safe(history)
    return history


def _recommend_by_factor_similarity(product_id: str, top_n: int) -> pd.DataFrame:
    assets = load_assets()
    target = (
        assets.interactions_df.where(F.col("product_id") == product_id)
        .select("item_idx")
        .limit(1)
        .collect()
    )
    if not target:
        return pd.DataFrame()

    target_idx = int(target[0]["item_idx"])
    item_factors = _to_pandas_safe(assets.als_model.itemFactors)
    if item_factors.empty:
        return pd.DataFrame()

    vectors = {
        int(row["id"]): np.array(row["features"], dtype=float)
        for _, row in item_factors.iterrows()
    }
    if target_idx not in vectors:
        return pd.DataFrame()

    target_vec = vectors[target_idx]
    sims: list[tuple[int, float]] = []
    for idx, vec in vectors.items():
        if idx == target_idx:
            continue
        denom = np.linalg.norm(target_vec) * np.linalg.norm(vec)
        score = float(np.dot(target_vec, vec) / denom) if denom else 0.0
        sims.append((idx, score))

    sims = sorted(sims, key=lambda x: x[1], reverse=True)[:top_n]
    if not sims:
        return pd.DataFrame()

    spark = get_spark()
    sim_df = spark.createDataFrame(sims, schema="item_idx int, predicted_score double")
    return _attach_product_metadata(sim_df, "predicted_score")


def _recommend_by_copurchase(product_id: str, top_n: int) -> pd.DataFrame:
    baskets = DataLoader.load_spark_df("pattern_baskets")
    if "items" not in baskets.columns:
        return pd.DataFrame()

    candidates = (
        baskets.where(F.array_contains(F.col("items"), F.lit(product_id)))
        .select(F.explode("items").alias("product_id"))
        .where(F.col("product_id") != F.lit(product_id))
        .groupBy("product_id")
        .count()
        .orderBy(F.desc("count"))
        .limit(top_n)
    )

    assets = load_assets()
    return _to_pandas_safe(
        candidates.join(assets.products_df, "product_id", "left").withColumnRenamed(
            "count", "predicted_score"
        )
    )


def get_product_based_recommendations(
    product_id: str, top_n: int
) -> tuple[pd.DataFrame, str]:
    product_id = (product_id or "").strip()
    if not product_id:
        return pd.DataFrame(), "Vui long nhap product_id."

    sim_df = _recommend_by_factor_similarity(product_id, top_n)
    if not sim_df.empty:
        return sim_df, "Goi y theo do tuong dong latent factor (ALS item factors)."

    fallback_df = _recommend_by_copurchase(product_id, top_n)
    if not fallback_df.empty:
        return fallback_df, "Fallback theo co-purchase tu gio hang lich su."

    return pd.DataFrame(), "Khong tim thay goi y cho san pham nay."


def search_products(keyword: str, limit: int = 30) -> pd.DataFrame:
    assets = load_assets()
    query = (keyword or "").strip().lower()
    if not query:
        return _to_pandas_safe(
            assets.products_df.select(
                "product_id", "product_name", "product_category_name"
            ).limit(limit)
        )

    cond = (
        F.lower(F.col("product_id")).contains(query)
        | F.lower(F.col("product_name")).contains(query)
        | F.lower(F.col("product_category_name")).contains(query)
    )
    return _to_pandas_safe(
        assets.products_df.where(cond)
        .select("product_id", "product_name", "product_category_name")
        .limit(limit)
    )
