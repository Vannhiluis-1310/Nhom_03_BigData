from __future__ import annotations

import argparse
from pathlib import Path
from typing import Literal

from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    ChiSqSelector,
    HashingTF,
    IDF,
    OneHotEncoder,
    RegexTokenizer,
    StandardScaler,
    StopWordsRemover,
    StringIndexer,
    VectorAssembler,
    Word2Vec,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from nhom03_pyspark_project.config import FEATURES_DIR, GOLD_DIR
from nhom03_pyspark_project.data.common import write_parquet
from nhom03_pyspark_project.spark import build_spark


def _ensure_supervised_columns(df: DataFrame) -> DataFrame:
    # Alias old/new column names to the leakage-safe feature schema expected by pipelines.
    alias_candidates = {
        "items_per_order": ["order_item_count"],
        "max_payment_installments": ["payment_installments_max"],
        "order_gmv": ["order_item_value", "order_payment_value"],
    }
    out = df
    existing = set(out.columns)
    for target, candidates in alias_candidates.items():
        if target in existing:
            continue
        for src in candidates:
            if src in existing:
                out = out.withColumn(target, F.col(src))
                existing.add(target)
                break

    numeric_defaults = [
        "items_per_order",
        "unique_products_per_order",
        "unique_categories_per_order",
        "max_payment_installments",
        "same_state_any_seller_flag",
        "avg_product_weight_g",
        "order_freight_value",
        "order_gmv",
    ]
    for c in numeric_defaults:
        if c not in out.columns:
            out = out.withColumn(c, F.lit(0.0))
        out = out.withColumn(c, F.coalesce(F.col(c).cast("double"), F.lit(0.0)))

    if "customer_state" not in out.columns:
        out = out.withColumn("customer_state", F.lit("unknown"))
    out = out.withColumn(
        "customer_state",
        F.coalesce(F.col("customer_state").cast("string"), F.lit("unknown")),
    )

    if "review_text" not in out.columns:
        if "review_comment_message" in out.columns:
            out = out.withColumn(
                "review_text",
                F.coalesce(F.col("review_comment_message").cast("string"), F.lit("")),
            )
        else:
            out = out.withColumn("review_text", F.lit(""))
    out = out.withColumn(
        "review_text",
        F.coalesce(F.col("review_text").cast("string"), F.lit("")),
    )

    if "is_low_review" in out.columns:
        out = out.withColumn(
            "is_low_review",
            F.coalesce(F.col("is_low_review").cast("double"), F.lit(0.0)),
        )

    return out


def _build_text_stages(
    text_method: Literal["tfidf", "word2vec"],
) -> list:
    tokenizer = RegexTokenizer(
        inputCol="review_text",
        outputCol="tokens",
        pattern="[^A-Za-z0-9]+",
        minTokenLength=2,
    )
    remover = StopWordsRemover(inputCol="tokens", outputCol="tokens_clean")

    if text_method == "word2vec":
        text_vectorizer = Word2Vec(
            inputCol="tokens_clean",
            outputCol="text_features",
            vectorSize=100,
            minCount=2,
        )
        return [tokenizer, remover, text_vectorizer]

    hashing_tf = HashingTF(
        inputCol="tokens_clean",
        outputCol="text_tf",
        numFeatures=1 << 14,
    )
    idf = IDF(inputCol="text_tf", outputCol="text_features")
    return [tokenizer, remover, hashing_tf, idf]


def _build_classification_pipeline(
    text_method: Literal["tfidf", "word2vec"] = "tfidf",
    use_chisq_selector: bool = True,
    chi_top_k: int = 300,
) -> Pipeline:
    num_cols = [
        "items_per_order",
        "unique_products_per_order",
        "unique_categories_per_order",
        "max_payment_installments",
        "same_state_any_seller_flag",
    ]
    cat_cols = ["customer_state"]
    text_stages = _build_text_stages(text_method)

    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in cat_cols
    ]
    encoders = [
        OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_ohe") for c in cat_cols
    ]

    assembler = VectorAssembler(
        inputCols=num_cols + [f"{c}_ohe" for c in cat_cols] + ["text_features"],
        outputCol="features_raw",
        handleInvalid="keep",
    )

    stages = [*indexers, *encoders, *text_stages, assembler]
    scaler_input_col = "features_raw"

    if use_chisq_selector:
        selector = ChiSqSelector(
            featuresCol="features_raw",
            outputCol="features_selected",
            labelCol="is_low_review",
            numTopFeatures=chi_top_k,
        )
        stages.append(selector)
        scaler_input_col = "features_selected"

    scaler = StandardScaler(
        inputCol=scaler_input_col,
        outputCol="features",
        withMean=False,
        withStd=True,
    )
    stages.append(scaler)
    return Pipeline(stages=stages)


def _build_regression_pipeline() -> Pipeline:
    num_cols = [
        "items_per_order",
        "unique_products_per_order",
        "unique_categories_per_order",
        "avg_product_weight_g",
        "max_payment_installments",
        "same_state_any_seller_flag",
        "order_freight_value",
    ]
    cat_cols = ["customer_state"]
    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in cat_cols
    ]
    encoders = [
        OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_ohe") for c in cat_cols
    ]
    assembler = VectorAssembler(
        inputCols=num_cols + [f"{c}_ohe" for c in cat_cols],
        outputCol="features_raw",
        handleInvalid="keep",
    )
    scaler = StandardScaler(
        inputCol="features_raw", outputCol="features", withMean=False, withStd=True
    )
    return Pipeline(stages=[*indexers, *encoders, assembler, scaler])


def _transform_and_keep(df: DataFrame, model, cols: list[str]) -> DataFrame:
    out = model.transform(df)
    return out.select(*cols)


def build_features_safe(
    spark: SparkSession,
    gold_dir: Path,
    features_dir: Path,
    classification_text_method: Literal["tfidf", "word2vec"] = "tfidf",
    use_chisq_selector: bool = True,
    chi_top_k: int = 300,
) -> None:
    c_train = spark.read.parquet(str(gold_dir / "classification_base_train"))
    c_val = spark.read.parquet(str(gold_dir / "classification_base_val"))
    c_test = spark.read.parquet(str(gold_dir / "classification_base_test"))

    c_train = _ensure_supervised_columns(c_train)
    c_val = _ensure_supervised_columns(c_val)
    c_test = _ensure_supervised_columns(c_test)

    # Keep text field for MLlib text feature extraction, remove raw review score label leakage.
    c_train = c_train.drop("review_score")
    c_val = c_val.drop("review_score")
    c_test = c_test.drop("review_score")

    c_pipe = _build_classification_pipeline(
        text_method=classification_text_method,
        use_chisq_selector=use_chisq_selector,
        chi_top_k=chi_top_k,
    )
    c_model = c_pipe.fit(c_train)
    c_keep = ["order_id", "is_low_review", "features"]
    c_train_fe = _transform_and_keep(c_train, c_model, c_keep).withColumnRenamed(
        "is_low_review", "label"
    )
    c_val_fe = _transform_and_keep(c_val, c_model, c_keep).withColumnRenamed(
        "is_low_review", "label"
    )
    c_test_fe = _transform_and_keep(c_test, c_model, c_keep).withColumnRenamed(
        "is_low_review", "label"
    )

    r_train = spark.read.parquet(str(gold_dir / "regression_base_train"))
    r_val = spark.read.parquet(str(gold_dir / "regression_base_val"))
    r_test = spark.read.parquet(str(gold_dir / "regression_base_test"))

    r_train = _ensure_supervised_columns(r_train)
    r_val = _ensure_supervised_columns(r_val)
    r_test = _ensure_supervised_columns(r_test)

    r_pipe = _build_regression_pipeline()
    r_model = r_pipe.fit(r_train)
    r_keep = ["order_id", "order_gmv", "features"]
    r_train_fe = _transform_and_keep(r_train, r_model, r_keep).withColumnRenamed(
        "order_gmv", "label"
    )
    r_val_fe = _transform_and_keep(r_val, r_model, r_keep).withColumnRenamed(
        "order_gmv", "label"
    )
    r_test_fe = _transform_and_keep(r_test, r_model, r_keep).withColumnRenamed(
        "order_gmv", "label"
    )

    clustering_base = spark.read.parquet(str(gold_dir / "clustering_base"))
    cnum = ["recency_days", "frequency_orders", "monetary_value", "avg_items_per_order"]
    c_asm = VectorAssembler(
        inputCols=cnum, outputCol="features_raw", handleInvalid="skip"
    )
    c_scale = StandardScaler(
        inputCol="features_raw", outputCol="features", withMean=False, withStd=True
    )
    c_pipe2 = Pipeline(stages=[c_asm, c_scale])
    c_model2 = c_pipe2.fit(clustering_base)
    clustering_fe = c_model2.transform(clustering_base).select(
        "customer_unique_id", "features"
    )

    als_train = spark.read.parquet(str(gold_dir / "als_base_train")).select(
        "customer_unique_id",
        "product_id",
        "user_idx",
        "item_idx",
        F.col("implicit_rating").alias("rating"),
    )
    als_val = spark.read.parquet(str(gold_dir / "als_base_val")).select(
        "customer_unique_id",
        "product_id",
        "user_idx",
        "item_idx",
        F.col("implicit_rating").alias("rating"),
    )
    als_test = spark.read.parquet(str(gold_dir / "als_base_test")).select(
        "customer_unique_id",
        "product_id",
        "user_idx",
        "item_idx",
        F.col("implicit_rating").alias("rating"),
    )
    als_ready = spark.read.parquet(str(gold_dir / "als_base")).select(
        "customer_unique_id",
        "product_id",
        "user_idx",
        "item_idx",
        F.col("implicit_rating").alias("rating"),
    )

    fpgrowth_ready = spark.read.parquet(str(gold_dir / "fpgrowth_base")).select(
        "order_id", "items"
    )

    outputs = {
        "classification_train": c_train_fe,
        "classification_val": c_val_fe,
        "classification_test": c_test_fe,
        "regression_train": r_train_fe,
        "regression_val": r_val_fe,
        "regression_test": r_test_fe,
        "clustering_fe": clustering_fe,
        "als_train": als_train,
        "als_val": als_val,
        "als_test": als_test,
        "als_ready": als_ready,
        "fpgrowth_ready": fpgrowth_ready,
    }
    for name, df in outputs.items():
        write_parquet(df, features_dir / name)
        print(f"[write] features/{name} rows={df.count():,}")

    c_model.write().overwrite().save(
        str(features_dir / "models" / "classification_fe_pipeline")
    )
    r_model.write().overwrite().save(
        str(features_dir / "models" / "regression_fe_pipeline")
    )
    c_model2.write().overwrite().save(
        str(features_dir / "models" / "clustering_fe_pipeline")
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build leakage-safe features from split gold datasets"
    )
    parser.add_argument("--gold-dir", type=Path, default=GOLD_DIR)
    parser.add_argument("--features-dir", type=Path, default=FEATURES_DIR)
    parser.add_argument(
        "--classification-text-method",
        choices=["tfidf", "word2vec"],
        default="tfidf",
        help="Text feature method for classification pipeline",
    )
    parser.add_argument(
        "--disable-chisq-selector",
        action="store_true",
        help="Disable ChiSqSelector stage in classification pipeline",
    )
    parser.add_argument(
        "--chi-top-k",
        type=int,
        default=300,
        help="Number of top features kept by ChiSqSelector",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = build_spark("nhom03_build_features_safe")
    try:
        build_features_safe(
            spark=spark,
            gold_dir=args.gold_dir,
            features_dir=args.features_dir,
            classification_text_method=args.classification_text_method,
            use_chisq_selector=not args.disable_chisq_selector,
            chi_top_k=args.chi_top_k,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
