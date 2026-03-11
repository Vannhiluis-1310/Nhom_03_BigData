#!/usr/bin/env python3
"""
Execute ChiSquareTest and generate reports for UTIL-08 requirement.
This script runs outside of Jupyter to generate the required CSV and JSON outputs.
"""

import sys
import os
from pathlib import Path
import json
import warnings

warnings.filterwarnings("ignore")

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(PROJECT_ROOT))

print("=" * 60)
print("ChiSquareTest Report Generator - UTIL-08")
print("=" * 60)

# Check PySpark availability
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql import functions as F
    from pyspark.ml.stat import ChiSquareTest
    from pyspark.ml.feature import StringIndexer, VectorAssembler

    print("✓ PySpark modules imported successfully")
except ImportError as e:
    print(f"✗ PySpark import error: {e}")
    print("  Installing required packages...")
    import subprocess

    subprocess.check_call([sys.executable, "-m", "pip", "install", "pyspark", "-q"])
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql import functions as F
    from pyspark.ml.stat import ChiSquareTest
    from pyspark.ml.feature import StringIndexer, VectorAssembler

    print("✓ PySpark installed and imported")

# Initialize SparkSession
print("\n[1/6] Initializing SparkSession...")
spark = (
    SparkSession.builder.appName("ChiSquareTest_Reports")
    .master("local[2]")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("✓ SparkSession initialized")

# Set paths
GOLD_DIR = PROJECT_ROOT / "data" / "processed" / "gold" / "classification_base"
REPORT_DIR = PROJECT_ROOT / "reports" / "chisquare"
REPORT_DIR.mkdir(parents=True, exist_ok=True)
print(f"✓ Report directory: {REPORT_DIR}")

# Load data
print("\n[2/6] Loading classification_base dataset...")
if not GOLD_DIR.exists():
    print(f"✗ Gold data directory not found: {GOLD_DIR}")
    sys.exit(1)

try:
    df = spark.read.parquet(str(GOLD_DIR))
    total_rows = df.count()
    print(f"✓ Loaded {total_rows:,} rows")
    print(f"  Columns: {', '.join(df.columns[:5])}...")
except Exception as e:
    print(f"✗ Error loading data: {e}")
    sys.exit(1)

# Check required columns
print("\n[3/6] Checking required columns...")
required_cols = ["customer_state", "is_low_review", "review_score"]
missing_cols = [col for col in required_cols if col not in df.columns]
if missing_cols:
    print(f"✗ Missing columns: {missing_cols}")
    print(f"  Available columns: {df.columns}")
    sys.exit(1)
print("✓ All required columns present")

# Index categorical features
print("\n[4/6] Indexing categorical features...")
state_indexer = StringIndexer(
    inputCol="customer_state", outputCol="state_idx", handleInvalid="keep"
)
df_indexed = state_indexer.fit(df).transform(df)
distinct_states = df.select("customer_state").distinct().count()
print(f"✓ Indexed {distinct_states} distinct states")

# Prepare labels - filter out null values first
print("\n[4.5/6] Filtering out null values...")
df_indexed = df_indexed.filter(
    F.col("is_low_review").isNotNull() & F.col("review_score").isNotNull()
)
filtered_count = df_indexed.count()
print(
    f"✓ Filtered to {filtered_count:,} rows (removed {total_rows - filtered_count:,} rows with nulls)"
)

df_indexed = df_indexed.withColumn(
    "is_low_review_label", F.col("is_low_review").cast("double")
)
df_indexed = df_indexed.withColumn(
    "review_score_label", F.col("review_score").cast("double")
)
df_indexed = df_indexed.withColumn(
    "review_score_label", F.col("review_score").cast("double")
)

# Assemble features
assembler = VectorAssembler(
    inputCols=["state_idx"], outputCol="features", handleInvalid="keep"
)
df_vector = assembler.transform(df_indexed)
df_test = df_vector.select(
    "order_id", "features", "is_low_review_label", "review_score_label"
)
print("✓ Features assembled")

# Run ChiSquareTest - Binary Target
print("\n[5/6] Running ChiSquareTest for binary target (is_low_review)...")
result_binary = ChiSquareTest.test(df_test, "features", "is_low_review_label").head()
p_value_binary = float(result_binary.pValues[0])
dof_binary = int(result_binary.degreesOfFreedom[0])
chi2_stat_binary = float(result_binary.statistics[0])
is_significant_binary = p_value_binary < 0.05

print(f"  p-value: {p_value_binary:.6f}")
print(f"  chi2 statistic: {chi2_stat_binary:.4f}")
print(f"  degrees of freedom: {dof_binary}")
print(f"  significant (α=0.05): {is_significant_binary}")

# Run ChiSquareTest - Multi-class Target
print("\n[6/6] Running ChiSquareTest for multi-class target (review_score)...")
result_multi = ChiSquareTest.test(df_test, "features", "review_score_label").head()
p_value_multi = float(result_multi.pValues[0])
dof_multi = int(result_multi.degreesOfFreedom[0])
chi2_stat_multi = float(result_multi.statistics[0])
is_significant_multi = p_value_multi < 0.05

print(f"  p-value: {p_value_multi:.6f}")
print(f"  chi2 statistic: {chi2_stat_multi:.4f}")
print(f"  degrees of freedom: {dof_multi}")
print(f"  significant (α=0.05): {is_significant_multi}")

# Generate CSV Report
print("\n[Output] Generating CSV report...")
import pandas as pd
from datetime import datetime

results_data = [
    {
        "test_name": "ChiSquare_Binary",
        "feature": "customer_state",
        "target_variable": "is_low_review",
        "p_value": p_value_binary,
        "degrees_of_freedom": dof_binary,
        "chi2_statistic": chi2_stat_binary,
        "is_significant_05": is_significant_binary,
        "interpretation": "Significant association"
        if is_significant_binary
        else "No significant association",
    },
    {
        "test_name": "ChiSquare_Multiclass",
        "feature": "customer_state",
        "target_variable": "review_score",
        "p_value": p_value_multi,
        "degrees_of_freedom": dof_multi,
        "chi2_statistic": chi2_stat_multi,
        "is_significant_05": is_significant_multi,
        "interpretation": "Significant association"
        if is_significant_multi
        else "No significant association",
    },
]

results_df = pd.DataFrame(results_data)
csv_path = REPORT_DIR / "chisquare_results.csv"
results_df.to_csv(csv_path, index=False)
print(f"✓ CSV saved: {csv_path}")

# Generate JSON Summary
print("\n[Output] Generating JSON summary...")
significant_count = sum([is_significant_binary, is_significant_multi])
total_tests = 2

json_summary = {
    "test_method": "ChiSquareTest",
    "library": "PySpark MLlib (pyspark.ml.stat)",
    "feature_tested": "customer_state",
    "total_tests": total_tests,
    "significant_results": significant_count,
    "alpha": 0.05,
    "execution_timestamp": datetime.now().isoformat(),
    "dataset_info": {
        "source": "classification_base",
        "total_rows": total_rows,
        "distinct_states": distinct_states,
    },
    "results": [
        {
            "test_id": 1,
            "target": "is_low_review",
            "target_type": "binary",
            "p_value": p_value_binary,
            "chi2_statistic": chi2_stat_binary,
            "degrees_of_freedom": dof_binary,
            "significant_at_05": is_significant_binary,
            "interpretation": "Significant association"
            if is_significant_binary
            else "No significant association",
        },
        {
            "test_id": 2,
            "target": "review_score",
            "target_type": "multiclass",
            "p_value": p_value_multi,
            "chi2_statistic": chi2_stat_multi,
            "degrees_of_freedom": dof_multi,
            "significant_at_05": is_significant_multi,
            "interpretation": "Significant association"
            if is_significant_multi
            else "No significant association",
        },
    ],
    "summary": {
        "significant_tests": significant_count,
        "total_tests": total_tests,
        "significance_rate": significant_count / total_tests,
        "conclusion": f"{significant_count}/{total_tests} tests show significant association between customer_state and review outcomes",
    },
}

json_path = REPORT_DIR / "chisquare_summary.json"
with open(json_path, "w", encoding="utf-8") as f:
    json.dump(json_summary, f, indent=2, ensure_ascii=False)
print(f"✓ JSON saved: {json_path}")

# Summary
print("\n" + "=" * 60)
print("EXECUTION COMPLETE")
print("=" * 60)
print(f"\nTest Results Summary:")
print(
    f"  Binary (is_low_review): p={p_value_binary:.6f}, significant={is_significant_binary}"
)
print(
    f"  Multi-class (review_score): p={p_value_multi:.6f}, significant={is_significant_multi}"
)
print(f"\nSignificant tests: {significant_count}/{total_tests}")
print(f"\nOutput files:")
print(f"  - {csv_path}")
print(f"  - {json_path}")

# Stop Spark
spark.stop()
print("\n✓ SparkSession stopped")
print("=" * 60)
