from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_parquet(df: DataFrame, output_dir: Path, mode: str = "overwrite") -> None:
    ensure_dir(output_dir.parent)
    df.write.mode(mode).parquet(str(output_dir))


def log_count(df: DataFrame, name: str) -> None:
    print(f"[{name}] rows={df.count():,}")
