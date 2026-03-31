from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Generator, Iterable

import pandas as pd
import streamlit as st
from pyspark.sql import SparkSession

from utils.config import (
    LOG_DIR,
    MODELS_DIR,
    RAW_DIR,
    REPORTS_DIR,
    ROOT,
    resolve_retrain_script,
    resolve_script_list,
)
from utils.data_loader import DataLoader
from utils.logger import get_logger
from utils.model_loader import list_model_availability
from utils.spark_helper import check_spark_connection

logger = get_logger("admin_service")

EXPECTED_RAW_FILES = {
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv",
}


def _runtime_python() -> str:
    # Windows
    venv_python_win = ROOT / ".venv" / "Scripts" / "python.exe"
    if venv_python_win.exists():
        return str(venv_python_win)
    # macOS / Linux
    venv_python_unix = ROOT / ".venv" / "bin" / "python"
    if venv_python_unix.exists():
        return str(venv_python_unix)
    return sys.executable


ACTIVITY_LOG = LOG_DIR / "activity_log.csv"
ERROR_LOG = LOG_DIR / "error_log.csv"


@dataclass
class ValidationResult:
    filename: str
    valid: bool
    message: str
    rows: int
    columns: list[str]
    preview: pd.DataFrame


def _ensure_log_dir() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def append_activity(action: str, status: str, detail: str = "") -> None:
    _ensure_log_dir()
    row = pd.DataFrame(
        [
            {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "action": action,
                "status": status,
                "detail": detail,
            }
        ]
    )
    if ACTIVITY_LOG.exists():
        row.to_csv(ACTIVITY_LOG, mode="a", header=False, index=False)
    else:
        row.to_csv(ACTIVITY_LOG, index=False)


def append_error(detail: str) -> None:
    _ensure_log_dir()
    row = pd.DataFrame(
        [
            {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "error": detail[:8000],
            }
        ]
    )
    if ERROR_LOG.exists():
        row.to_csv(ERROR_LOG, mode="a", header=False, index=False)
    else:
        row.to_csv(ERROR_LOG, index=False)


def validate_uploaded_file(file_obj) -> ValidationResult:
    name = file_obj.name
    suffix = Path(name).suffix.lower()
    try:
        if suffix == ".csv":
            preview = pd.read_csv(file_obj).head(20)
        elif suffix == ".parquet":
            preview = pd.read_parquet(file_obj).head(20)
        else:
            return ValidationResult(
                name, False, "Dinh dang file khong duoc ho tro.", 0, [], pd.DataFrame()
            )

        message = "Hop le"
        if name not in EXPECTED_RAW_FILES:
            expected = ", ".join(sorted(EXPECTED_RAW_FILES))
            message = (
                "Hop le (file ngoai bo input chuan). Da luu vao data/raw, "
                "nhung preprocessing mac dinh chi doc cac ten file chuan: "
                f"{expected}"
            )

        return ValidationResult(
            filename=name,
            valid=True,
            message=message,
            rows=len(preview),
            columns=preview.columns.tolist(),
            preview=preview,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("Validate file that bai: %s", exc)
        return ValidationResult(name, False, str(exc), 0, [], pd.DataFrame())


def save_uploaded_files(files: Iterable) -> list[Path]:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    saved: list[Path] = []
    for file_obj in files:
        out_path = RAW_DIR / file_obj.name
        out_path.write_bytes(file_obj.getbuffer())
        saved.append(out_path)
    append_activity("upload_data", "success", f"Saved {len(saved)} files")
    return saved


def _stream_subprocess(
    command: list[str], cwd: Path, env: dict[str, str] | None = None
) -> Generator[str, None, None]:
    logger.info("Run command: %s", " ".join(command))
    process = subprocess.Popen(
        command,
        cwd=str(cwd),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert process.stdout is not None
    for line in iter(process.stdout.readline, ""):
        if line:
            yield line.rstrip("\n")
    process.stdout.close()
    return_code = process.wait()
    yield f"[EXIT_CODE] {return_code}"


def run_notebooks(notebook_paths: list[Path]) -> Generator[str, None, None]:
    runtime_python = _runtime_python()
    for script_path in notebook_paths:
        if not script_path.exists():
            yield f"[ERROR] Khong tim thay script/notebook: {script_path}"
            continue
        cmd_env = os.environ.copy()
        src_dir = ROOT / "src"
        if src_dir.exists():
            existing = cmd_env.get("PYTHONPATH", "")
            cmd_env["PYTHONPATH"] = (
                f"{src_dir}{os.pathsep}{existing}" if existing else str(src_dir)
            )
        if script_path.suffix.lower() == ".py":
            yield f"[INFO] Dang chay python script: {script_path.name}"
            cmd = [runtime_python, str(script_path)]
            cwd = ROOT
        else:
            yield f"[INFO] Dang chay notebook: {script_path.name}"
            cmd = [
                runtime_python,
                "-m",
                "jupyter",
                "nbconvert",
                "--to",
                "notebook",
                "--execute",
                "--inplace",
                "--ExecutePreprocessor.kernel_name=python3",
                str(script_path),
            ]
            cwd = ROOT
        for line in _stream_subprocess(cmd, cwd=cwd, env=cmd_env):
            yield line


def run_preprocessing_pipeline() -> Generator[str, None, None]:
    append_activity("preprocessing_pipeline", "start", "Start preprocessing")
    notebooks = resolve_script_list("preprocessing")
    if not notebooks:
        yield "[ERROR] Khong tim thay cau hinh script preprocessing."
        append_activity("preprocessing_pipeline", "failed", "No scripts configured")
        return
    failed = False
    tail_logs: list[str] = []
    for line in run_notebooks(notebooks):
        yield line
        tail_logs.append(line)
        if len(tail_logs) > 50:
            tail_logs.pop(0)
        if line.startswith("[EXIT_CODE]") and line.strip() != "[EXIT_CODE] 0":
            failed = True
            break
    if failed:
        append_activity("preprocessing_pipeline", "failed", "Notebook execution failed")
        append_error(
            "Preprocessing pipeline failed. Last logs:\n" + "\n".join(tail_logs)
        )
        yield "[ERROR] Preprocessing pipeline failed."
    else:
        append_activity("preprocessing_pipeline", "success", "Completed preprocessing")


def run_reporting_pipeline() -> Generator[str, None, None]:
    append_activity("reporting_pipeline", "start", "Start reporting")
    scripts = resolve_script_list("reporting")
    if not scripts:
        yield "[ERROR] Khong tim thay cau hinh script reporting."
        append_activity("reporting_pipeline", "failed", "No scripts configured")
        return
    failed = False
    for line in run_notebooks(scripts):
        yield line
        if line.startswith("[EXIT_CODE]") and line.strip() != "[EXIT_CODE] 0":
            failed = True
            break
    if failed:
        append_activity("reporting_pipeline", "failed", "Reporting execution failed")
        append_error("Reporting pipeline failed. Check execution logs.")
        yield "[ERROR] Reporting pipeline failed."
    else:
        append_activity("reporting_pipeline", "success", "Completed reporting")


def _model_versions_root() -> Path:
    return MODELS_DIR / "_versions"


def snapshot_family_version(model_family: str) -> Path:
    family_dir = MODELS_DIR / (
        "pattern" if model_family == "pattern_mining" else model_family
    )
    if not family_dir.exists():
        raise FileNotFoundError(f"Missing family dir: {family_dir}")
    version = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = _model_versions_root() / model_family / version
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(family_dir, dest)
    append_activity("snapshot_model", "success", f"{model_family}:{version}")
    return dest


def list_family_versions(model_family: str) -> pd.DataFrame:
    root = _model_versions_root() / model_family
    if not root.exists():
        return pd.DataFrame(columns=["version", "path", "created_at"])
    rows = []
    for p in sorted(root.iterdir(), reverse=True):
        if p.is_dir():
            rows.append(
                {
                    "version": p.name,
                    "path": str(p),
                    "created_at": datetime.fromtimestamp(p.stat().st_mtime).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                }
            )
    return pd.DataFrame(rows)


def rollback_family_version(model_family: str, version: str) -> None:
    src = _model_versions_root() / model_family / version
    if not src.exists():
        raise FileNotFoundError(f"Version not found: {src}")
    target = MODELS_DIR / (
        "pattern" if model_family == "pattern_mining" else model_family
    )
    backup = (
        target.parent
        / f"{target.name}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )
    if target.exists():
        shutil.move(str(target), str(backup))
    shutil.copytree(src, target)
    append_activity("rollback_model", "success", f"{model_family}:{version}")


def run_retrain_pipeline(model_family: str) -> Generator[str, None, None]:
    append_activity("retrain_model", "start", model_family)
    script = resolve_retrain_script(model_family)
    if script is None:
        msg = f"[ERROR] Khong co script retrain cho family={model_family}."
        append_activity("retrain_model", "failed", msg)
        yield msg
        return
    failed = False
    for line in run_notebooks([script]):
        yield line
        if line.startswith("[EXIT_CODE]") and line.strip() != "[EXIT_CODE] 0":
            failed = True
            break
    if failed:
        append_activity("retrain_model", "failed", model_family)
        append_error(f"Retrain failed for family={model_family}")
        yield f"[ERROR] Retrain failed for family={model_family}"
        return
    version_path = snapshot_family_version(model_family)
    yield f"[INFO] Snapshot version saved: {version_path}"
    for line in run_reporting_pipeline():
        yield line
    append_activity("retrain_model", "success", model_family)


def scan_models() -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    if not MODELS_DIR.exists():
        return pd.DataFrame(rows)

    for family_dir in MODELS_DIR.iterdir():
        if not family_dir.is_dir():
            continue
        if family_dir.name.startswith("_"):
            continue
        family = "pattern_mining" if family_dir.name == "pattern" else family_dir.name
        for model_dir in family_dir.iterdir():
            if not model_dir.is_dir():
                continue
            metric_prefix = "pattern" if family == "pattern_mining" else family
            metric_json = (
                REPORTS_DIR / "model_metrics" / f"{metric_prefix}_{model_dir.name}.json"
            )
            metrics = {}
            if metric_json.exists():
                try:
                    metrics = json.loads(metric_json.read_text(encoding="utf-8"))
                except Exception:  # noqa: BLE001
                    metrics = {}
            rows.append(
                {
                    "family": family,
                    "model": model_dir.name,
                    "path": str(model_dir),
                    "last_modified": datetime.fromtimestamp(
                        model_dir.stat().st_mtime
                    ).strftime("%Y-%m-%d %H:%M"),
                    "metrics": json.dumps(metrics, ensure_ascii=False),
                }
            )
    return pd.DataFrame(rows)


def compare_model_metrics(model_family: str) -> pd.DataFrame:
    metrics_dir = REPORTS_DIR / "model_metrics"
    if not metrics_dir.exists():
        return pd.DataFrame()

    family_prefix = "pattern" if model_family == "pattern_mining" else model_family
    rows: list[dict[str, object]] = []
    for file_path in metrics_dir.glob(f"{family_prefix}_*.json"):
        try:
            payload = json.loads(file_path.read_text(encoding="utf-8"))
            payload["file"] = file_path.name
            rows.append(payload)
        except Exception:  # noqa: BLE001
            continue
    return pd.DataFrame(rows)


def system_status() -> dict[str, object]:
    spark_ok, spark_message = check_spark_connection()
    data_status = DataLoader.check_required_datasets()
    model_status = list_model_availability()
    return {
        "spark_ok": spark_ok,
        "spark_message": spark_message,
        "data_status": data_status,
        "model_status": model_status,
    }


def load_activity_log() -> pd.DataFrame:
    if ACTIVITY_LOG.exists():
        return pd.read_csv(ACTIVITY_LOG)
    return pd.DataFrame(columns=["timestamp", "action", "status", "detail"])


def load_error_log() -> pd.DataFrame:
    if ERROR_LOG.exists():
        return pd.read_csv(ERROR_LOG)
    return pd.DataFrame(columns=["timestamp", "error"])


def clear_streamlit_cache() -> None:
    active = SparkSession.getActiveSession()
    if active is not None:
        try:
            active.stop()
        except Exception:  # noqa: BLE001
            pass
    st.cache_data.clear()
    st.cache_resource.clear()
    append_activity("clear_cache", "success", "Cache cleared")
