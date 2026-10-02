from pathlib import Path

from src.ingestion.spark_session import create_spark_session
from src.ingestion.utils import ingest_dataset


PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
BRONZE_DATA_DIR = PROJECT_ROOT / "data" / "bronze"


if __name__ == "__main__":

    spark = create_spark_session()

    datasets = [
        ("olist_customers_dataset.csv", "customers"),
        ("olist_geolocation_dataset.csv", "geolocation"),
        ("olist_order_items_dataset.csv", "order_items"),
        ("olist_order_payments_dataset.csv", "order_payments"),
        ("olist_order_reviews_dataset.csv", "order_reviews"),
        ("olist_orders_dataset.csv", "orders"),
        ("olist_products_dataset.csv", "products"),
        ("olist_sellers_dataset.csv", "sellers"),
        ("product_category_name_translation.csv", "product_category_translation"),
    ]

    for csv_filename, bronze_name in datasets:

        print(f"\nStarting ingestion: {csv_filename}")

        df = ingest_dataset(
            spark,
            RAW_DATA_DIR,
            BRONZE_DATA_DIR,
            csv_filename,
            bronze_name
        )

        print(f"Completed ingestion: {bronze_name}")
        print("-" * 60)

    spark.stop()