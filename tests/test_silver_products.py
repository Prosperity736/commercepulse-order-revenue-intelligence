from pathlib import Path

from src.ingestion.spark_session import create_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SILVER_PRODUCTS_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "products"
)


def main():

    spark = create_spark_session()

    df = spark.read.parquet(
        str(SILVER_PRODUCTS_PATH)
    )

    # Row count
    row_count = df.count()

    assert row_count == 32951, (
        f"Expected 32951 rows, got {row_count}"
    )

    # Column count
    column_count = len(df.columns)

    assert column_count == 9, (
        f"Expected 9 columns, got {column_count}"
    )

    # product_id uniqueness
    duplicate_product_ids = (
        df.groupBy("product_id")
        .count()
        .filter("count > 1")
        .count()
    )

    assert duplicate_product_ids == 0, (
        "Duplicate product_id values found"
    )

    # No negative product measurements
    numeric_columns = [
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ]

    for column in numeric_columns:

        negative_count = (
            df.filter(f"{column} < 0")
            .count()
        )

        assert negative_count == 0, (
            f"Negative values found in {column}"
        )

    # Product ID should not be NULL
    null_product_ids = (
        df.filter("product_id IS NULL")
        .count()
    )

    assert null_product_ids == 0, (
        "NULL product_id values found"
    )

    print("✓ Silver products validation passed")
    print(f"✓ Rows: {row_count}")
    print(f"✓ Columns: {column_count}")
    print("✓ product_id values are unique")
    print("✓ No negative product measurements")
    print("✓ product_id contains no NULL values")

    spark.stop()


if __name__ == "__main__":
    main()