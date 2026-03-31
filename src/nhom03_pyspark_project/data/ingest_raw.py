from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from nhom03_pyspark_project.config import BRONZE_DIR, RAW_DIR, TABLE_FILE_MAP
from nhom03_pyspark_project.data.common import log_count, write_parquet
from nhom03_pyspark_project.spark import build_spark


def read_csv_table(spark: SparkSession, csv_path: Path) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("multiLine", False)
        .csv(str(csv_path))
    )


def ingest_raw_to_bronze(spark: SparkSession, raw_dir: Path, bronze_dir: Path) -> None:
    for table_name, file_name in TABLE_FILE_MAP.items():
        csv_path = raw_dir / file_name
        if not csv_path.exists():
            raise FileNotFoundError(f"Missing raw file: {csv_path}")

        df = read_csv_table(spark, csv_path)
        log_count(df, f"raw.{table_name}")
        write_parquet(df, bronze_dir / table_name)
        print(f"[write] {table_name} -> {bronze_dir / table_name}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest Olist raw CSV to bronze parquet"
    )
    parser.add_argument("--raw-dir", type=Path, default=RAW_DIR)
    parser.add_argument("--bronze-dir", type=Path, default=BRONZE_DIR)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = build_spark("nhom03_ingest_raw")
    try:
        ingest_raw_to_bronze(spark, args.raw_dir, args.bronze_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
