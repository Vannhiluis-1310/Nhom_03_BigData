from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from utils.data_loader import DataLoader
from utils.logger import get_logger
from utils.model_loader import load_pattern_mining_artifacts

logger = get_logger("pattern_mining_service")


@dataclass
class PatternResult:
    itemsets_pdf: pd.DataFrame
    rules_pdf: pd.DataFrame
    insights: list[str]
    warning: str | None
    metrics: dict[str, Any]


def _to_tuple(values: Any) -> tuple[str, ...]:
    if values is None:
        return tuple()
    if isinstance(values, (list, tuple, set)):
        return tuple(str(v) for v in values)
    return (str(values),)


def _load_product_category_map() -> dict[str, str]:
    products = DataLoader.load_pandas_sample("product_lookup", limit=200000)
    if products.empty or "product_id" not in products.columns:
        return {}
    if "product_category_name" not in products.columns:
        products["product_category_name"] = "unknown_category"
    return dict(
        zip(
            products["product_id"].astype(str),
            products["product_category_name"].fillna("unknown_category").astype(str),
        )
    )


def _map_items(
    items: tuple[str, ...], level: str, mapping: dict[str, str]
) -> tuple[str, ...]:
    if level == "Theo Product ID":
        return tuple(sorted(set(items)))
    mapped = [mapping.get(item, "unknown_category") for item in items]
    return tuple(sorted(set(mapped)))


def _normalize_itemsets(
    df: pd.DataFrame, level: str, mapping: dict[str, str]
) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["items_tuple"] = out["items"].apply(_to_tuple)
    out["itemsets"] = out["items_tuple"].apply(lambda x: _map_items(x, level, mapping))
    out["item_count"] = out["itemsets"].apply(len)
    grouped = (
        out.groupby("itemsets", as_index=False)
        .agg(
            support=("support", "max"),
            frequency=("freq", "sum"),
            item_count=("item_count", "max"),
        )
        .sort_values("support", ascending=False)
    )
    grouped["itemsets_text"] = grouped["itemsets"].apply(lambda x: ", ".join(x))
    return grouped


def _normalize_rules(
    df: pd.DataFrame, level: str, mapping: dict[str, str]
) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["antecedent_tuple"] = out["antecedent"].apply(_to_tuple)
    out["consequent_tuple"] = out["consequent"].apply(_to_tuple)
    out["antecedent_norm"] = out["antecedent_tuple"].apply(
        lambda x: _map_items(x, level, mapping)
    )
    out["consequent_norm"] = out["consequent_tuple"].apply(
        lambda x: _map_items(x, level, mapping)
    )
    grouped = (
        out.groupby(["antecedent_norm", "consequent_norm"], as_index=False)
        .agg(
            support=("support", "max"),
            confidence=("confidence", "max"),
            lift=("lift", "max"),
        )
        .sort_values(["lift", "confidence"], ascending=False)
    )
    grouped["rule_text"] = grouped.apply(
        lambda row: (
            f"{', '.join(row['antecedent_norm'])} -> {', '.join(row['consequent_norm'])}"
        ),
        axis=1,
    )
    return grouped


def _build_insights(rules_pdf: pd.DataFrame) -> list[str]:
    if rules_pdf.empty:
        return ["Khong co luat manh theo bo loc hien tai."]
    top = rules_pdf.sort_values(["lift", "confidence"], ascending=False).head(3)
    insights = []
    for _, row in top.iterrows():
        insights.append(
            "Neu khach mua "
            f"{', '.join(row['antecedent_norm'])} thi nen goi y them {', '.join(row['consequent_norm'])} "
            f"(confidence={row['confidence']:.2f}, lift={row['lift']:.2f})."
        )
    return insights


def get_pattern_outputs(
    mining_level: str,
    min_support: float,
    min_confidence: float,
    min_itemsets: int,
    top_rules: int,
) -> PatternResult:
    artifacts = load_pattern_mining_artifacts()
    metrics = artifacts.get("metrics", {})
    itemsets_pdf = artifacts["itemsets_df"].toPandas()
    rules_pdf = artifacts["rules_df"].toPandas()

    basket_count = int(metrics.get("transactions", 0))
    if basket_count <= 0:
        baskets = DataLoader.load_spark_df("pattern_baskets")
        basket_count = baskets.count()

    if "support" not in itemsets_pdf.columns:
        if "freq" in itemsets_pdf.columns and basket_count > 0:
            itemsets_pdf["support"] = itemsets_pdf["freq"] / basket_count
        else:
            itemsets_pdf["support"] = 0.0

    available_min_support = (
        float(itemsets_pdf["support"].min()) if not itemsets_pdf.empty else 0.0
    )
    available_min_conf = (
        float(rules_pdf["confidence"].min()) if not rules_pdf.empty else 0.0
    )
    warning = None
    if min_support < available_min_support or min_confidence < available_min_conf:
        warning = (
            "Threshold dang nho hon output da luu. "
            "Ket qua hien thi duoc loc tu artifact hien co; neu can day du hon, vui long retrain tren trang Admin."
        )

    mapping = _load_product_category_map()
    itemsets_norm = _normalize_itemsets(itemsets_pdf, mining_level, mapping)
    rules_norm = _normalize_rules(rules_pdf, mining_level, mapping)

    filtered_itemsets = itemsets_norm[
        (itemsets_norm["support"] >= min_support)
        & (itemsets_norm["item_count"] >= min_itemsets)
    ].copy()

    if filtered_itemsets.empty and not itemsets_norm.empty:
        itemsets_with_size = itemsets_norm[itemsets_norm["item_count"] >= min_itemsets]
        if not itemsets_with_size.empty:
            suggested_support = float(itemsets_with_size["support"].max())
            hint = (
                f"Khong co itemset voi minSupport={min_support:.3f}, minItemsets={min_itemsets}. "
                f"Thu ha minSupport <= {suggested_support:.4f}."
            )
            warning = f"{warning} {hint}" if warning else hint

    filtered_rules = rules_norm[
        (rules_norm["support"] >= min_support)
        & (rules_norm["confidence"] >= min_confidence)
    ].copy()
    filtered_rules = filtered_rules.head(top_rules)

    insights = _build_insights(filtered_rules)
    logger.info(
        "Pattern mining filter done | level=%s | itemsets=%d | rules=%d",
        mining_level,
        len(filtered_itemsets),
        len(filtered_rules),
    )

    return PatternResult(filtered_itemsets, filtered_rules, insights, warning, metrics)
