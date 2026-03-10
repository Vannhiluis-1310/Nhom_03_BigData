from __future__ import annotations

import logging
import os
import platform
import sys

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def build_spark(app_name: str) -> SparkSession:
    python_exec = os.environ.get("PYSPARK_PYTHON") or sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", python_exec)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python_exec)

    if platform.system() == "Windows" and not os.environ.get("HADOOP_HOME"):
        logger.warning(
            "HADOOP_HOME chua duoc set. Tren Windows, Spark can HADOOP_HOME "
            "tro toi thu muc chua winutils.exe. "
            "Xem: https://cwiki.apache.org/confluence/display/HADOOP2/WindowsProblems"
        )

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
