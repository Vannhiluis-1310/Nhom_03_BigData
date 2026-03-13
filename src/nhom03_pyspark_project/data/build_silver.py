from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from nhom03_pyspark_project.config import BRONZE_DIR, SILVER_DIR
from nhom03_pyspark_project.data.common import log_count, write_parquet
from nhom03_pyspark_project.spark import build_spark


def read_parquet_table(spark: SparkSession, base_dir: Path, table: str) -> DataFrame:
    path = base_dir / table
    if not path.exists():
        raise FileNotFoundError(f"Missing parquet table: {path}")
    return spark.read.parquet(str(path))


def cast_orders(df: DataFrame) -> DataFrame:
    ts_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    out = df
    for col_name in ts_cols:
        out = out.withColumn(col_name, F.to_timestamp(F.col(col_name)))
    return out


def cast_order_items(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("order_item_id", F.col("order_item_id").cast("int"))
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("freight_value", F.col("freight_value").cast("double"))
        .withColumn("shipping_limit_date", F.to_timestamp(F.col("shipping_limit_date")))
    )


def cast_order_payments(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("payment_sequential", F.col("payment_sequential").cast("int"))
        .withColumn("payment_installments", F.col("payment_installments").cast("int"))
        .withColumn("payment_value", F.col("payment_value").cast("double"))
    )


def cast_order_reviews(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("review_score", F.col("review_score").cast("int"))
        .withColumn(
            "review_creation_date", F.to_timestamp(F.col("review_creation_date"))
        )
        .withColumn(
            "review_answer_timestamp", F.to_timestamp(F.col("review_answer_timestamp"))
        )
    )


def cast_products(df: DataFrame) -> DataFrame:
    numeric_cols = [
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ]
    out = df
    for col_name in numeric_cols:
        out = out.withColumn(col_name, F.col(col_name).cast("double"))
    return out


def cast_geolocation(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("geolocation_lat", F.col("geolocation_lat").cast("double"))
        .withColumn("geolocation_lng", F.col("geolocation_lng").cast("double"))
        .withColumn(
            "geolocation_zip_code_prefix",
            F.col("geolocation_zip_code_prefix").cast("int"),
        )
    )


def dedup_geolocation(df: DataFrame) -> DataFrame:
    return df.groupBy("geolocation_zip_code_prefix").agg(
        F.avg("geolocation_lat").alias("geolocation_lat"),
        F.avg("geolocation_lng").alias("geolocation_lng"),
        F.first("geolocation_city", ignorenulls=True).alias("geolocation_city"),
        F.first("geolocation_state", ignorenulls=True).alias("geolocation_state"),
    )


def build_silver_tables(
    spark: SparkSession, bronze_dir: Path, silver_dir: Path
) -> None:
    customers = read_parquet_table(spark, bronze_dir, "customers").dropDuplicates(
        ["customer_id"]
    )
    orders = cast_orders(
        read_parquet_table(spark, bronze_dir, "orders")
    ).dropDuplicates(["order_id"])
    order_items = cast_order_items(read_parquet_table(spark, bronze_dir, "order_items"))
    order_payments = cast_order_payments(
        read_parquet_table(spark, bronze_dir, "order_payments")
    )
    order_reviews = cast_order_reviews(
        read_parquet_table(spark, bronze_dir, "order_reviews")
    )
    products = cast_products(
        read_parquet_table(spark, bronze_dir, "products")
    ).dropDuplicates(["product_id"])
    sellers = read_parquet_table(spark, bronze_dir, "sellers").dropDuplicates(
        ["seller_id"]
    )
    geolocation = dedup_geolocation(
        cast_geolocation(read_parquet_table(spark, bronze_dir, "geolocation"))
    )
    category_translation = read_parquet_table(
        spark, bronze_dir, "category_translation"
    ).dropDuplicates(["product_category_name"])

    tables = {
        "customers": customers,
        "orders": orders,
        "order_items": order_items,
        "order_payments": order_payments,
        "order_reviews": order_reviews,
        "products": products,
        "sellers": sellers,
        "geolocation": geolocation,
        "category_translation": category_translation,
    }

    for table_name, df in tables.items():
        log_count(df, f"silver.{table_name}")
        write_parquet(df, silver_dir / table_name)
        print(f"[write] {table_name} -> {silver_dir / table_name}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build silver parquet tables from bronze"
    )
    parser.add_argument("--bronze-dir", type=Path, default=BRONZE_DIR)
    parser.add_argument("--silver-dir", type=Path, default=SILVER_DIR)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = build_spark("nhom03_build_silver")
    try:
        build_silver_tables(spark, args.bronze_dir, args.silver_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
