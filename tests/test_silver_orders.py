from pathlib import Path

from src.ingestion.spark_session import create_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SILVER_ORDERS_PATH = PROJECT_ROOT / "data" / "silver" / "orders"


def test_silver_orders():

    spark = create_spark_session()

    # Read Silver orders
    orders_df = spark.read.parquet(
        str(SILVER_ORDERS_PATH)
    )

    # 1. Check row count
    row_count = orders_df.count()

    assert row_count == 99441, (
        f"Expected 99441 rows, but found {row_count}"
    )

    # 2. Check expected columns
    expected_columns = [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]

    assert orders_df.columns == expected_columns, (
        f"Unexpected columns: {orders_df.columns}"
    )

    # 3. Check order_id uniqueness
    unique_order_ids = (
        orders_df
        .select("order_id")
        .distinct()
        .count()
    )

    assert unique_order_ids == row_count, (
        "Duplicate order_id values were found"
    )

    print("✓ Silver orders validation passed")
    print(f"✓ Rows: {row_count}")
    print(f"✓ Columns: {len(orders_df.columns)}")
    print("✓ order_id values are unique")

    spark.stop()


if __name__ == "__main__":
    test_silver_orders()