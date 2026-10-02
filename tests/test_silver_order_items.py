from pathlib import Path

from src.ingestion.spark_session import create_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SILVER_ORDER_ITEMS_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "order_items"
)


def test_silver_order_items():

    spark = create_spark_session()

    order_items_df = spark.read.parquet(
        str(SILVER_ORDER_ITEMS_PATH)
    )

    # 1. Check row count
    row_count = order_items_df.count()

    assert row_count == 112650, (
        f"Expected 112650 rows, but found {row_count}"
    )

    # 2. Check expected columns
    expected_columns = [
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
    ]

    assert order_items_df.columns == expected_columns, (
        f"Unexpected columns: {order_items_df.columns}"
    )

    # 3. Check composite key uniqueness
    unique_items = (
        order_items_df
        .select(
            "order_id",
            "order_item_id"
        )
        .distinct()
        .count()
    )

    assert unique_items == row_count, (
        "Duplicate (order_id, order_item_id) "
        "pairs were found"
    )

    # 4. Check price is non-negative
    negative_prices = (
        order_items_df
        .filter(order_items_df.price < 0)
        .count()
    )

    assert negative_prices == 0, (
        f"Found {negative_prices} negative prices"
    )

    # 5. Check freight value is non-negative
    negative_freight = (
        order_items_df
        .filter(order_items_df.freight_value < 0)
        .count()
    )

    assert negative_freight == 0, (
        f"Found {negative_freight} negative freight values"
    )

    print("✓ Silver order_items validation passed")
    print(f"✓ Rows: {row_count}")
    print(f"✓ Columns: {len(order_items_df.columns)}")
    print("✓ (order_id, order_item_id) pairs are unique")
    print("✓ No negative prices")
    print("✓ No negative freight values")

    spark.stop()


if __name__ == "__main__":
    test_silver_order_items()