from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
import streamlit as st
from pyspark.sql import DataFrame, functions as F

from utils.data_loader import DataLoader
from utils.logger import get_logger

logger = get_logger("dashboard_service")


def _revenue_expression(df: DataFrame):
    if "order_item_value" in df.columns and "order_freight_value" in df.columns:
        return F.coalesce(F.col("order_item_value"), F.lit(0.0)) + F.coalesce(
            F.col("order_freight_value"), F.lit(0.0)
        )
    if "order_gmv" in df.columns:
        return F.coalesce(F.col("order_gmv"), F.lit(0.0))
    return F.lit(0.0)


def load_dashboard_base() -> DataFrame:
    return DataLoader.load_spark_df("dashboard_base")


@st.cache_data(show_spinner=False)
def load_filter_options() -> dict[str, Any]:
    df = load_dashboard_base()

    min_max = df.select(
        F.to_date(F.min("order_purchase_timestamp")).alias("min_d"),
        F.to_date(F.max("order_purchase_timestamp")).alias("max_d"),
    ).first()

    states = [
        row[0]
        for row in df.select("customer_state")
        .where(F.col("customer_state").isNotNull())
        .dropDuplicates(["customer_state"])
        .orderBy("customer_state")
        .collect()
    ]

    cities = [
        row[0]
        for row in df.select("customer_city")
        .where(F.col("customer_city").isNotNull())
        .dropDuplicates(["customer_city"])
        .orderBy("customer_city")
        .limit(1000)
        .collect()
    ]

    pay = (
        df.select(F.explode_outer("payment_type_set").alias("payment_type"))
        .where(F.col("payment_type").isNotNull())
        .dropDuplicates(["payment_type"])
        .orderBy("payment_type")
        .toPandas()
    )
    payment_types = pay["payment_type"].tolist() if not pay.empty else []

    return {
        "min_date": min_max["min_d"],
        "max_date": min_max["max_d"],
        "states": states,
        "cities": cities,
        "payment_types": payment_types,
    }


def apply_filters(
    df: DataFrame,
    start_date: date,
    end_date: date,
    states: list[str],
    cities: list[str],
    payment_types: list[str],
) -> DataFrame:
    logger.info("Ap dung bo loc dashboard")
    filtered = df.where(
        (F.to_date("order_purchase_timestamp") >= F.lit(str(start_date)))
        & (F.to_date("order_purchase_timestamp") <= F.lit(str(end_date)))
    )
    if states:
        filtered = filtered.where(F.col("customer_state").isin(states))
    if cities:
        filtered = filtered.where(F.col("customer_city").isin(cities))
    if payment_types:
        cond = F.lit(False)
        for payment in payment_types:
            cond = cond | F.array_contains(F.col("payment_type_set"), F.lit(payment))
        filtered = filtered.where(cond)
    return filtered


def compute_kpis(df: DataFrame) -> dict[str, float]:
    revenue_expr = _revenue_expression(df)
    row = df.agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum(revenue_expr).alias("total_revenue"),
        F.countDistinct("customer_unique_id").alias("total_customers"),
        F.avg("review_score").alias("avg_review_score"),
    ).first()
    return {
        "total_orders": float(row["total_orders"] or 0),
        "total_revenue": float(row["total_revenue"] or 0),
        "total_customers": float(row["total_customers"] or 0),
        "avg_review_score": float(row["avg_review_score"] or 0),
    }


def build_revenue_trend(df: DataFrame) -> pd.DataFrame:
    revenue_expr = _revenue_expression(df)
    return (
        df.withColumn("month", F.date_format("order_purchase_timestamp", "yyyy-MM"))
        .groupBy("month")
        .agg(
            F.countDistinct("order_id").alias("so_don"),
            F.round(F.sum(revenue_expr), 2).alias("doanh_thu"),
        )
        .orderBy("month")
        .toPandas()
    )


def build_top_categories(df: DataFrame, limit: int = 10) -> pd.DataFrame:
    return (
        df.select(F.explode_outer("category_set").alias("category"))
        .where(F.col("category").isNotNull())
        .groupBy("category")
        .count()
        .orderBy(F.desc("count"))
        .limit(limit)
        .toPandas()
    )


def build_payment_distribution(df: DataFrame) -> pd.DataFrame:
    return (
        df.select(F.explode_outer("payment_type_set").alias("payment_type"))
        .where(F.col("payment_type").isNotNull())
        .groupBy("payment_type")
        .count()
        .orderBy(F.desc("count"))
        .toPandas()
    )


def build_order_status(df: DataFrame) -> pd.DataFrame:
    if "order_status" not in df.columns:
        return pd.DataFrame(columns=["order_status", "count"])
    return df.groupBy("order_status").count().orderBy(F.desc("count")).toPandas()


def build_state_distribution(df: DataFrame) -> pd.DataFrame:
    return (
        df.groupBy("customer_state")
        .count()
        .orderBy(F.desc("count"))
        .where(F.col("customer_state").isNotNull())
        .toPandas()
    )


def recent_orders(df: DataFrame, limit: int = 5) -> pd.DataFrame:
    columns = [
        "order_id",
        "customer_id",
        "customer_state",
        "customer_city",
        "order_purchase_timestamp",
        "order_status",
    ]
    if "order_gmv" in df.columns:
        columns.append("order_gmv")
    return (
        df.select(*[c for c in columns if c in df.columns])
        .orderBy(F.desc("order_purchase_timestamp"))
        .limit(limit)
        .toPandas()
    )
