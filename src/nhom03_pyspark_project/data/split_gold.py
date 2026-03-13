from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from nhom03_pyspark_project.config import GOLD_DIR
from nhom03_pyspark_project.data.common import write_parquet
from nhom03_pyspark_project.spark import build_spark


def _split_by_time(
    df: DataFrame, ts_col: str
) -> tuple[DataFrame, DataFrame, DataFrame]:
    ts_num_col = "_ts_split_unix"
    with_ts = df.withColumn(ts_num_col, F.col(ts_col).cast("long"))
    q = with_ts.where(F.col(ts_num_col).isNotNull()).approxQuantile(
        ts_num_col, [0.7, 0.85], 0.0
    )
    if len(q) != 2:
        raise ValueError(f"Cannot compute quantiles for {ts_col}")
    q70, q85 = q
    train = with_ts.where(
        F.col(ts_num_col).isNull() | (F.col(ts_num_col) <= F.lit(q70))
    ).drop(ts_num_col)
    val = with_ts.where(
        (F.col(ts_num_col) > F.lit(q70)) & (F.col(ts_num_col) <= F.lit(q85))
    ).drop(ts_num_col)
    test = with_ts.where(F.col(ts_num_col) > F.lit(q85)).drop(ts_num_col)
    return train, val, test


def _split_by_hash(
    df: DataFrame, key_col: str
) -> tuple[DataFrame, DataFrame, DataFrame]:
    with_bucket = df.withColumn(
        "_split_bucket", (F.abs(F.hash(F.col(key_col))) % 20).cast("int")
    )
    train = with_bucket.where(F.col("_split_bucket") < 14).drop("_split_bucket")
    val = with_bucket.where(
        (F.col("_split_bucket") >= 14) & (F.col("_split_bucket") < 17)
    ).drop("_split_bucket")
    test = with_bucket.where(F.col("_split_bucket") >= 17).drop("_split_bucket")
    return train, val, test


def _split_als_interactions(df: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    # Split by interaction key to avoid cold-start on user-only split.
    if "user_idx" in df.columns and "item_idx" in df.columns:
        interaction_key = F.concat_ws(
            "_",
            F.col("user_idx").cast("string"),
            F.col("item_idx").cast("string"),
        )
        with_bucket = df.withColumn(
            "_split_bucket", (F.abs(F.hash(interaction_key)) % 20).cast("int")
        )
        train = with_bucket.where(F.col("_split_bucket") < 14).drop("_split_bucket")
        val = with_bucket.where(
            (F.col("_split_bucket") >= 14) & (F.col("_split_bucket") < 17)
        ).drop("_split_bucket")
        test = with_bucket.where(F.col("_split_bucket") >= 17).drop("_split_bucket")
        return train, val, test

    return _split_by_hash(df, "customer_unique_id")


def split_gold(spark: SparkSession, gold_dir: Path) -> None:
    classification_base = spark.read.parquet(str(gold_dir / "classification_base"))
    regression_base = spark.read.parquet(str(gold_dir / "regression_base"))
    als_base = spark.read.parquet(str(gold_dir / "als_base"))

    c_train, c_val, c_test = _split_by_time(
        classification_base, "order_purchase_timestamp"
    )
    r_train, r_val, r_test = _split_by_time(regression_base, "order_purchase_timestamp")
    a_train, a_val, a_test = _split_als_interactions(als_base)

    outputs = {
        "classification_base_train": c_train,
        "classification_base_val": c_val,
        "classification_base_test": c_test,
        "regression_base_train": r_train,
        "regression_base_val": r_val,
        "regression_base_test": r_test,
        "als_base_train": a_train,
        "als_base_val": a_val,
        "als_base_test": a_test,
    }

    for name, part in outputs.items():
        write_parquet(part, gold_dir / name)
        print(f"[write] {name} rows={part.count():,}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Split gold datasets into train/val/test"
    )
    parser.add_argument("--gold-dir", type=Path, default=GOLD_DIR)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = build_spark("nhom03_split_gold")
    try:
        split_gold(spark, args.gold_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
