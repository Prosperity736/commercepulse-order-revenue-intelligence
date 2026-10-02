from pathlib import Path

from src.ingestion.spark_session import create_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SILVER_SELLERS_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "sellers"
)


def test_silver_sellers():

    spark = create_spark_session()

    df = spark.read.parquet(
        str(SILVER_SELLERS_PATH)
    )

    # Expected row count
    row_count = df.count()
    assert row_count == 3095, (
        f"Expected 3095 rows, got {row_count}"
    )

    # Expected columns
    expected_columns = [
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
    ]

    assert df.columns == expected_columns, (
        f"Unexpected columns: {df.columns}"
    )

    # seller_id must be unique
    unique_seller_ids = (
        df.select("seller_id")
        .distinct()
        .count()
    )

    assert unique_seller_ids == row_count, (
        "seller_id values are not unique"
    )

    # seller_id must not be NULL
    null_seller_ids = df.filter(
        df.seller_id.isNull()
    ).count()

    assert null_seller_ids == 0, (
        "seller_id contains NULL values"
    )

    # ZIP code should not be negative
    negative_zip_codes = df.filter(
        df.seller_zip_code_prefix < 0
    ).count()

    assert negative_zip_codes == 0, (
        "seller_zip_code_prefix contains negative values"
    )

    print("✓ Silver sellers validation passed")
    print(f"✓ Rows: {row_count}")
    print(f"✓ Columns: {len(df.columns)}")
    print("✓ seller_id values are unique")
    print("✓ seller_id contains no NULL values")
    print("✓ No negative ZIP codes")

    spark.stop()


if __name__ == "__main__":
    test_silver_sellers()