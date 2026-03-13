from __future__ import annotations

import argparse
from pathlib import Path

import yaml
from pyspark.sql import DataFrame, SparkSession

from nhom03_pyspark_project.config import (
    BRONZE_DIR,
    PROJECT_ROOT,
    RAW_DIR,
    RAW_INPUT_MAPPING_PATH,
    TABLE_FILE_MAP,
)
from nhom03_pyspark_project.data.common import log_count, write_parquet
from nhom03_pyspark_project.spark import build_spark


def read_csv_table(spark: SparkSession, csv_path: Path) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("multiLine", False)
        .csv(str(csv_path))
    )


def _resolve_mapping_path(path_like: str | Path, raw_dir: Path) -> Path:
    path = Path(path_like)
    if path.is_absolute():
        return path
    if str(path).startswith("data/") or str(path).startswith("data\\"):
        return PROJECT_ROOT / path
    return raw_dir / path


def load_raw_input_mapping(raw_dir: Path) -> dict[str, Path]:
    mapping: dict[str, Path] = {}
    if RAW_INPUT_MAPPING_PATH.exists():
        payload = yaml.safe_load(RAW_INPUT_MAPPING_PATH.read_text(encoding="utf-8")) or {}
        if not isinstance(payload, dict):
            raise ValueError(
                f"Invalid raw input mapping format: {RAW_INPUT_MAPPING_PATH}"
            )
        for table_name, path_like in payload.items():
            if table_name not in TABLE_FILE_MAP or not path_like:
                continue
            mapping[table_name] = _resolve_mapping_path(path_like, raw_dir)

    for table_name, file_name in TABLE_FILE_MAP.items():
        mapping.setdefault(table_name, raw_dir / file_name)
    return mapping


def ingest_raw_to_bronze(spark: SparkSession, raw_dir: Path, bronze_dir: Path) -> None:
    source_map = load_raw_input_mapping(raw_dir)
    for table_name in TABLE_FILE_MAP:
        csv_path = source_map[table_name]
        if not csv_path.exists():
            raise FileNotFoundError(f"Missing raw file: {csv_path}")

        df = read_csv_table(spark, csv_path)
        log_count(df, f"raw.{table_name}")
        write_parquet(df, bronze_dir / table_name)
        print(f"[source] {table_name} <- {csv_path}")
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
