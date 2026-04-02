from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
BRONZE_DIR = PROCESSED_DIR / "bronze"
SILVER_DIR = PROCESSED_DIR / "silver"
GOLD_DIR = PROCESSED_DIR / "gold"
FEATURES_DIR = PROCESSED_DIR / "features"
MODELS_DIR = PROJECT_ROOT / "models"
REPORTS_DIR = PROJECT_ROOT / "reports"
MODEL_REGISTRY_PATH = MODELS_DIR / "registry.json"
CONFIG_DIR = PROJECT_ROOT / "config"
RAW_INPUT_MAPPING_PATH = CONFIG_DIR / "raw_input_mapping.yaml"

TABLE_FILE_MAP = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}
