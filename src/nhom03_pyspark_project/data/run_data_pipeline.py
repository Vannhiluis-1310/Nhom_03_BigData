from __future__ import annotations

from nhom03_pyspark_project.config import BRONZE_DIR, GOLD_DIR, RAW_DIR, SILVER_DIR
from nhom03_pyspark_project.data.build_gold import build_gold_datasets
from nhom03_pyspark_project.data.build_silver import build_silver_tables
from nhom03_pyspark_project.data.ingest_raw import ingest_raw_to_bronze
from nhom03_pyspark_project.data.split_gold import split_gold
from nhom03_pyspark_project.features.build_features_safe import build_features_safe
from nhom03_pyspark_project.spark import build_spark


def main() -> None:
    spark = build_spark("nhom03_run_data_pipeline")
    try:
        ingest_raw_to_bronze(spark=spark, raw_dir=RAW_DIR, bronze_dir=BRONZE_DIR)
        build_silver_tables(spark=spark, bronze_dir=BRONZE_DIR, silver_dir=SILVER_DIR)
        build_gold_datasets(spark=spark, silver_dir=SILVER_DIR, gold_dir=GOLD_DIR)
        split_gold(spark=spark, gold_dir=GOLD_DIR)
        build_features_safe(
            spark=spark, gold_dir=GOLD_DIR, features_dir=GOLD_DIR.parent / "features"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
