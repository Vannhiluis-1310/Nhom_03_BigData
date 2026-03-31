from __future__ import annotations

import os
import sys

import streamlit as st
from pyspark.sql import SparkSession

from utils.logger import get_logger

logger = get_logger("spark")


@st.cache_resource(show_spinner=False)
def get_spark() -> SparkSession:
    """Create and cache SparkSession for Streamlit app."""
    python_exec = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_exec
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exec

    active = SparkSession.getActiveSession()
    if active is not None:
        try:
            active.stop()
        except Exception:  # noqa: BLE001
            pass

    logger.info("Khoi tao SparkSession cho Streamlit app")
    logger.info("Dong bo Python cho Spark: %s", python_exec)
    spark = (
        SparkSession.builder.appName("olist_streamlit_demo")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.python.worker.reuse", "true")
        .config("spark.pyspark.python", python_exec)
        .config("spark.pyspark.driver.python", python_exec)
        .config("spark.executorEnv.PYSPARK_PYTHON", python_exec)
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def check_spark_connection() -> tuple[bool, str]:
    """Validate Spark health with a lightweight query."""
    try:
        spark = get_spark()
        spark.range(1).count()
        return True, "Spark da ket noi"
    except Exception as exc:  # noqa: BLE001
        logger.error("Spark khong san sang: %s", exc)
        return False, f"Khong ket noi duoc Spark: {exc}"


def python_runtime_info() -> dict[str, str]:
    """Return runtime information to debug Python version mismatch."""
    active = SparkSession.getActiveSession()
    arrow = ""
    reuse = ""
    if active is not None:
        try:
            arrow = active.conf.get("spark.sql.execution.arrow.pyspark.enabled", "")
            reuse = active.conf.get("spark.python.worker.reuse", "")
        except Exception:  # noqa: BLE001
            arrow = ""
            reuse = ""
    return {
        "sys.executable": sys.executable,
        "sys.version": sys.version.replace("\n", " "),
        "PYSPARK_PYTHON": os.environ.get("PYSPARK_PYTHON", ""),
        "PYSPARK_DRIVER_PYTHON": os.environ.get("PYSPARK_DRIVER_PYTHON", ""),
        "spark.arrow.enabled": arrow,
        "spark.python.worker.reuse": reuse,
    }
