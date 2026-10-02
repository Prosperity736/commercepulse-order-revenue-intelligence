from pathlib import Path

from pyspark.sql import functions as F

from src.ingestion.spark_session import create_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_DATA_DIR = PROJECT_ROOT / "data" / "bronze"


DATASETS = [
    "customers",
    "geolocation",
    "order_items",
    "order_payments",
    "order_reviews",
    "orders",
    "products",
    "sellers",
    "product_category_translation",
]


KEY_MAPPING = {
    "customers": ["customer_id"],
    "geolocation": [],
    "order_items": [],
    "order_payments": [],
    "order_reviews": ["review_id"],
    "orders": ["order_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "product_category_translation": [
        "product_category_name"
    ],
}


def validate_row_count(df):
    """
    Check the total number of rows.
    """

    row_count = df.count()

    print(f"Row count: {row_count:,}")

    if row_count == 0:
        print("WARNING: Dataset is empty.")


def validate_schema(df):
    """
    Display the DataFrame schema.
    """

    print("Schema:")
    df.printSchema()


def validate_nulls(df):
    """
    Count NULL values in every column.
    """

    null_counts = (
        df.select(
            [
                F.sum(
                    F.when(
                        F.col(column).isNull(),
                        1
                    ).otherwise(0)
                ).alias(column)
                for column in df.columns
            ]
        )
        .collect()[0]
        .asDict()
    )

    print("Null counts:")

    found_nulls = False

    for column, count in null_counts.items():

        if count > 0:
            found_nulls = True
            print(f"  {column}: {count:,}")

    if not found_nulls:
        print("  No NULL values found.")


def validate_duplicates(df):
    """
    Check for completely duplicated rows.
    """

    total_rows = df.count()
    distinct_rows = df.distinct().count()

    duplicate_rows = total_rows - distinct_rows

    print(f"Duplicate rows: {duplicate_rows:,}")


def validate_keys(df, key_columns):
    """
    Check key columns for NULL values
    and duplicate values.
    """

    for key in key_columns:

        if key not in df.columns:
            print(
                f"Key '{key}': column not found"
            )
            continue

        null_count = (
            df.filter(
                F.col(key).isNull()
            ).count()
        )

        duplicate_count = (
            df.groupBy(key)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        print(
            f"Key '{key}': "
            f"NULLs={null_count:,}, "
            f"duplicate values={duplicate_count:,}"
        )


if __name__ == "__main__":

    spark = create_spark_session()

    try:

        for dataset_name in DATASETS:

            print("\n" + "=" * 70)
            print(
                f"VALIDATING BRONZE DATASET: "
                f"{dataset_name}"
            )
            print("=" * 70)

            bronze_path = (
                BRONZE_DATA_DIR / dataset_name
            )

            if not bronze_path.exists():

                print(
                    f"ERROR: Bronze dataset not found: "
                    f"{bronze_path}"
                )

                continue

            df = spark.read.parquet(
                str(bronze_path)
            )

            print("\n--- ROW COUNT ---")
            validate_row_count(df)

            print("\n--- SCHEMA ---")
            validate_schema(df)

            print("\n--- NULL VALIDATION ---")
            validate_nulls(df)

            print("\n--- DUPLICATE VALIDATION ---")
            validate_duplicates(df)

            keys = KEY_MAPPING.get(
                dataset_name,
                []
            )

            if keys:

                print("\n--- KEY VALIDATION ---")

                validate_keys(
                    df,
                    keys
                )

    finally:

        spark.stop()

    print("\n" + "=" * 70)
    print("BRONZE VALIDATION COMPLETE")
    print("=" * 70)