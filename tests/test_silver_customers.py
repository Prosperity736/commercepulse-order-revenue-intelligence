from pathlib import Path

from src.ingestion.spark_session import create_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SILVER_CUSTOMERS_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "customers"
)


def test_silver_customers():

    spark = create_spark_session()

    customers_df = spark.read.parquet(
        str(SILVER_CUSTOMERS_PATH)
    )

    # 1. Check row count
    row_count = customers_df.count()

    assert row_count == 99441, (
        f"Expected 99441 rows, but found {row_count}"
    )

    # 2. Check expected columns
    expected_columns = [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    ]

    assert customers_df.columns == expected_columns, (
        f"Unexpected columns: {customers_df.columns}"
    )

    # 3. Check customer_id uniqueness
    unique_customer_ids = (
        customers_df
        .select("customer_id")
        .distinct()
        .count()
    )

    assert unique_customer_ids == row_count, (
        "Duplicate customer_id values were found"
    )

    print("✓ Silver customers validation passed")
    print(f"✓ Rows: {row_count}")
    print(f"✓ Columns: {len(customers_df.columns)}")
    print("✓ customer_id values are unique")

    spark.stop()


if __name__ == "__main__":
    test_silver_customers()