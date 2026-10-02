from pathlib import Path

from src.ingestion.spark_session import create_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[1]

BRONZE_DATA_DIR = PROJECT_ROOT / "data" / "bronze"


EXPECTED_COLUMNS = {
    "customers": [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    ],

    "geolocation": [
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng",
        "geolocation_city",
        "geolocation_state",
    ],

    "order_items": [
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
    ],

    "order_payments": [
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
    ],

    "order_reviews": [
        "review_id",
        "order_id",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_creation_date",
        "review_answer_timestamp",
    ],

    "orders": [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],

    "products": [
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ],

    "sellers": [
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
    ],

    "product_category_translation": [
        "product_category_name",
        "product_category_name_english",
    ],
}


def test_bronze_datasets():

    spark = create_spark_session()

    for dataset, expected_columns in EXPECTED_COLUMNS.items():

        bronze_path = BRONZE_DATA_DIR / dataset

        df = spark.read.parquet(str(bronze_path))

        actual_columns = df.columns

        missing_columns = [
            column
            for column in expected_columns
            if column not in actual_columns
        ]

        assert not missing_columns, (
            f"{dataset} is missing columns: {missing_columns}"
        )

        row_count = df.count()

        assert row_count > 0, (
            f"{dataset} contains no records"
        )

        print(
            f"✓ {dataset}: "
            f"{row_count} rows, "
            f"{len(actual_columns)} columns"
        )

    spark.stop()


if __name__ == "__main__":
    test_bronze_datasets()