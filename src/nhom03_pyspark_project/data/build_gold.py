from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from nhom03_pyspark_project.config import GOLD_DIR, SILVER_DIR
from nhom03_pyspark_project.data.common import log_count, write_parquet
from nhom03_pyspark_project.spark import build_spark


def read_table(spark: SparkSession, base_dir: Path, table_name: str) -> DataFrame:
    path = base_dir / table_name
    if not path.exists():
        raise FileNotFoundError(f"Missing table path: {path}")
    return spark.read.parquet(str(path))


def build_order_value(order_items: DataFrame) -> DataFrame:
    return order_items.groupBy("order_id").agg(
        F.sum("price").alias("order_item_value"),
        F.sum("freight_value").alias("order_freight_value"),
        F.count("*").alias("order_item_count"),
    )


def build_payment_summary(order_payments: DataFrame) -> DataFrame:
    return order_payments.groupBy("order_id").agg(
        F.sum("payment_value").alias("order_payment_value"),
        F.max("payment_installments").alias("payment_installments_max"),
        F.countDistinct("payment_type").alias("payment_type_count"),
    )


def build_review_summary(order_reviews: DataFrame) -> DataFrame:
    return order_reviews.groupBy("order_id").agg(
        F.max("review_score").alias("review_score"),
        F.first("review_comment_message", ignorenulls=True).alias(
            "review_comment_message"
        ),
    )


def build_classification_base(
    orders: DataFrame,
    customers: DataFrame,
    review_summary: DataFrame,
    order_value: DataFrame,
    payment_summary: DataFrame,
) -> DataFrame:
    base = (
        orders.join(customers, on="customer_id", how="left")
        .join(review_summary, on="order_id", how="left")
        .join(order_value, on="order_id", how="left")
        .join(payment_summary, on="order_id", how="left")
    )

    return (
        base.withColumn(
            "is_low_review", F.when(F.col("review_score") <= 3, 1).otherwise(0)
        )
        .withColumn("label_policy_version", F.lit("v1_review_leq_3"))
        .withColumn(
            "purchase_month", F.date_trunc("month", F.col("order_purchase_timestamp"))
        )
        .withColumn(
            "review_comment_message",
            F.coalesce(F.col("review_comment_message").cast("string"), F.lit("")),
        )
        .dropDuplicates(["order_id"])
    )


def build_regression_base(
    orders: DataFrame,
    customers: DataFrame,
    order_value: DataFrame,
    payment_summary: DataFrame,
) -> DataFrame:
    return (
        orders.join(customers, on="customer_id", how="left")
        .join(order_value, on="order_id", how="left")
        .join(payment_summary, on="order_id", how="left")
        .withColumn(
            "purchase_month", F.date_trunc("month", F.col("order_purchase_timestamp"))
        )
        .dropDuplicates(["order_id"])
    )


def build_clustering_base(
    orders: DataFrame,
    customers: DataFrame,
    order_value: DataFrame,
) -> DataFrame:
    order_level = (
        orders.join(customers, on="customer_id", how="left")
        .join(order_value, on="order_id", how="left")
        .select(
            "customer_unique_id",
            "order_id",
            "order_purchase_timestamp",
            "order_item_value",
            "order_item_count",
        )
    )

    anchor_date = order_level.agg(
        F.max("order_purchase_timestamp").alias("max_ts")
    ).collect()[0]["max_ts"]

    return order_level.groupBy("customer_unique_id").agg(
        F.datediff(F.lit(anchor_date), F.max("order_purchase_timestamp")).alias(
            "recency_days"
        ),
        F.countDistinct("order_id").alias("frequency_orders"),
        F.sum(F.coalesce(F.col("order_item_value"), F.lit(0.0))).alias(
            "monetary_value"
        ),
        F.avg(F.coalesce(F.col("order_item_count"), F.lit(0.0))).alias(
            "avg_items_per_order"
        ),
    )


def build_als_base(
    orders: DataFrame,
    customers: DataFrame,
    order_items: DataFrame,
) -> DataFrame:
    delivered_orders = orders.filter(F.col("order_status") == "delivered").select(
        "order_id", "customer_id"
    )
    interactions = (
        delivered_orders.join(
            customers.select("customer_id", "customer_unique_id"),
            on="customer_id",
            how="left",
        )
        .join(order_items.select("order_id", "product_id"), on="order_id", how="inner")
        .groupBy("customer_unique_id", "product_id")
        .agg(F.count("*").cast("double").alias("implicit_rating"))
    )

    user_window = Window.orderBy("customer_unique_id")
    item_window = Window.orderBy("product_id")

    return (
        interactions.withColumn("user_idx", F.dense_rank().over(user_window) - 1)
        .withColumn("item_idx", F.dense_rank().over(item_window) - 1)
        .select(
            "customer_unique_id",
            "product_id",
            "user_idx",
            "item_idx",
            "implicit_rating",
        )
    )


def build_fpgrowth_base(order_items: DataFrame) -> DataFrame:
    return order_items.groupBy("order_id").agg(
        F.collect_set("product_id").alias("items")
    )


def build_gold_datasets(spark: SparkSession, silver_dir: Path, gold_dir: Path) -> None:
    customers = read_table(spark, silver_dir, "customers")
    orders = read_table(spark, silver_dir, "orders")
    order_items = read_table(spark, silver_dir, "order_items")
    order_payments = read_table(spark, silver_dir, "order_payments")
    order_reviews = read_table(spark, silver_dir, "order_reviews")

    order_value = build_order_value(order_items)
    payment_summary = build_payment_summary(order_payments)
    review_summary = build_review_summary(order_reviews)

    classification_base = build_classification_base(
        orders=orders,
        customers=customers,
        review_summary=review_summary,
        order_value=order_value,
        payment_summary=payment_summary,
    )
    regression_base = build_regression_base(
        orders=orders,
        customers=customers,
        order_value=order_value,
        payment_summary=payment_summary,
    )
    clustering_base = build_clustering_base(
        orders=orders,
        customers=customers,
        order_value=order_value,
    )
    als_base = build_als_base(
        orders=orders,
        customers=customers,
        order_items=order_items,
    )
    fpgrowth_base = build_fpgrowth_base(order_items=order_items)

    outputs = {
        "classification_base": classification_base,
        "regression_base": regression_base,
        "clustering_base": clustering_base,
        "als_base": als_base,
        "fpgrowth_base": fpgrowth_base,
    }

    for name, df in outputs.items():
        log_count(df, f"gold.{name}")
        write_parquet(df, gold_dir / name)
        print(f"[write] {name} -> {gold_dir / name}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build task-specific gold datasets from silver"
    )
    parser.add_argument("--silver-dir", type=Path, default=SILVER_DIR)
    parser.add_argument("--gold-dir", type=Path, default=GOLD_DIR)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = build_spark("nhom03_build_gold")
    try:
        build_gold_datasets(spark, args.silver_dir, args.gold_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
