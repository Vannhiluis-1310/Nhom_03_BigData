from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
import streamlit as st
from pyspark.sql import DataFrame, functions as F

from utils.data_loader import DataLoader
from utils.logger import get_logger

logger = get_logger("eda_service")


def load_eda_base() -> DataFrame:
    return DataLoader.load_spark_df("eda_base")


@st.cache_data(show_spinner=False)
def load_filter_options() -> dict[str, Any]:
    df = load_eda_base()
    min_max = df.select(
        F.to_date(F.min("order_purchase_timestamp")).alias("min_date"),
        F.to_date(F.max("order_purchase_timestamp")).alias("max_date"),
    ).first()

    categories = (
        df.select(F.explode_outer("category_set").alias("category"))
        .where(F.col("category").isNotNull())
        .dropDuplicates(["category"])
        .orderBy("category")
        .limit(300)
        .toPandas()["category"]
        .tolist()
    )

    states = [
        row[0]
        for row in df.select("customer_state")
        .where(F.col("customer_state").isNotNull())
        .dropDuplicates(["customer_state"])
        .orderBy("customer_state")
        .collect()
    ]

    row = df.select(
        F.min("order_gmv").alias("min_price"),
        F.max("order_gmv").alias("max_price"),
    ).first()

    return {
        "min_date": min_max["min_date"],
        "max_date": min_max["max_date"],
        "categories": categories,
        "states": states,
        "min_price": float(row["min_price"] or 0.0),
        "max_price": float(row["max_price"] or 0.0),
    }


def apply_filters(
    base_df: DataFrame,
    start_date: date,
    end_date: date,
    categories: list[str],
    states: list[str],
    price_range: tuple[float, float],
) -> DataFrame:
    df = base_df.where(
        (F.to_date("order_purchase_timestamp") >= F.lit(str(start_date)))
        & (F.to_date("order_purchase_timestamp") <= F.lit(str(end_date)))
        & (F.col("order_gmv") >= F.lit(float(price_range[0])))
        & (F.col("order_gmv") <= F.lit(float(price_range[1])))
    )
    if categories:
        cond = F.lit(False)
        for category in categories:
            cond = cond | F.array_contains(F.col("category_set"), F.lit(category))
        df = df.where(cond)
    if states:
        df = df.where(F.col("customer_state").isin(states))
    logger.info("Da loc EDA data")
    return df


def granularity_to_pattern(granularity: str) -> str:
    mapping = {"Ngay": "yyyy-MM-dd", "Tuan": "yyyy-ww", "Thang": "yyyy-MM"}
    return mapping.get(granularity, "yyyy-MM")


def order_revenue_trend(df: DataFrame, granularity: str) -> pd.DataFrame:
    pattern = granularity_to_pattern(granularity)
    return (
        df.withColumn("period", F.date_format("order_purchase_timestamp", pattern))
        .groupBy("period")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.sum(F.coalesce(F.col("order_gmv"), F.lit(0.0))).alias("revenue"),
        )
        .orderBy("period")
        .toPandas()
    )


def revenue_by_category(df: DataFrame, granularity: str) -> pd.DataFrame:
    pattern = granularity_to_pattern(granularity)
    return (
        df.withColumn("period", F.date_format("order_purchase_timestamp", pattern))
        .select(
            "period",
            "order_id",
            "order_gmv",
            F.explode_outer("category_set").alias("category"),
        )
        .where(F.col("category").isNotNull())
        .groupBy("period", "category")
        .agg(F.sum("order_gmv").alias("revenue"))
        .orderBy("period")
        .toPandas()
    )


def new_vs_returning(df: DataFrame, granularity: str) -> pd.DataFrame:
    pattern = granularity_to_pattern(granularity)
    sequence = df.select("customer_unique_id", "order_purchase_timestamp").withColumn(
        "period", F.date_format("order_purchase_timestamp", pattern)
    )
    first_order = sequence.groupBy("customer_unique_id").agg(
        F.min("period").alias("first_period")
    )
    labeled = sequence.join(first_order, "customer_unique_id", "left").withColumn(
        "customer_type",
        F.when(F.col("period") == F.col("first_period"), F.lit("New")).otherwise(
            F.lit("Returning")
        ),
    )
    return (
        labeled.groupBy("period", "customer_type").count().orderBy("period").toPandas()
    )


def product_performance(df: DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    category_perf = (
        df.select(F.explode_outer("category_set").alias("category"), "order_gmv")
        .where(F.col("category").isNotNull())
        .groupBy("category")
        .agg(
            F.count("category").alias("order_count"),
            F.avg("order_gmv").alias("avg_order_value"),
        )
        .orderBy(F.desc("order_count"))
        .toPandas()
    )
    top = category_perf.head(10)
    bottom = category_perf.sort_values("order_count", ascending=True).head(10)
    return top, bottom


def review_distribution(
    df: DataFrame, granularity: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    pattern = granularity_to_pattern(granularity)
    score_dist = df.groupBy("review_score").count().orderBy("review_score").toPandas()
    avg_over_time = (
        df.withColumn("period", F.date_format("order_purchase_timestamp", pattern))
        .groupBy("period")
        .agg(F.avg("review_score").alias("avg_review_score"))
        .orderBy("period")
        .toPandas()
    )
    return score_dist, avg_over_time


def delivery_performance(df: DataFrame) -> pd.DataFrame:
    return (
        df.withColumn(
            "delivery_days",
            F.datediff(
                F.col("order_delivered_customer_date"),
                F.col("order_purchase_timestamp"),
            ),
        )
        .where(F.col("delivery_days").isNotNull())
        .where(F.col("delivery_days") >= 0)
        .select("delivery_days", "customer_state")
        .limit(30000)
        .toPandas()
    )


def payment_analysis(
    filtered_orders_df: DataFrame, granularity: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    pattern = granularity_to_pattern(granularity)
    payments = DataLoader.load_spark_df("order_payments")
    joined = filtered_orders_df.select("order_id", "order_purchase_timestamp").join(
        payments, "order_id", "left"
    )

    trend = (
        joined.withColumn("period", F.date_format("order_purchase_timestamp", pattern))
        .filter(F.col("payment_type").isNotNull())
        .filter(F.col("payment_type") != "not_defined")
        .groupBy("period", "payment_type")
        .count()
        .orderBy("period")
        .toPandas()
    )

    installments = (
        joined.groupBy("payment_installments")
        .count()
        .orderBy("payment_installments")
        .toPandas()
    )
    return trend, installments


def filtered_table(df: DataFrame, limit: int = 2000) -> pd.DataFrame:
    cols = [
        "order_id",
        "customer_unique_id",
        "customer_state",
        "order_status",
        "order_purchase_timestamp",
        "order_gmv",
        "review_score",
    ]
    keep_cols = [c for c in cols if c in df.columns]
    return (
        df.select(*keep_cols)
        .orderBy(F.desc("order_purchase_timestamp"))
        .limit(limit)
        .toPandas()
    )


def correlation_matrix(pdf: pd.DataFrame) -> pd.DataFrame:
    if pdf.empty:
        return pd.DataFrame()
    numeric_cols = pdf.select_dtypes(include=["number"]).columns.tolist()
    if len(numeric_cols) < 2:
        return pd.DataFrame()
    return pdf[numeric_cols].corr(numeric_only=True)
