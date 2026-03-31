from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession


def build_spark(app_name: str) -> SparkSession:
    python_exec = os.environ.get("PYSPARK_PYTHON") or sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", python_exec)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python_exec)

    return (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.pyspark.python", os.environ["PYSPARK_PYTHON"])
        .config("spark.pyspark.driver.python", os.environ["PYSPARK_DRIVER_PYTHON"])
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.python.worker.reuse", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
