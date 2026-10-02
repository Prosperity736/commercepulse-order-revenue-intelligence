from src.ingestion.spark_session import create_spark_session
from pathlib import Path
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SILVER_ORDER_REVIEWS_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "order_reviews"
)


if __name__ == "__main__":

    spark = create_spark_session()

    # ---------------------------------------------------------
    # Load Silver order reviews
    # ---------------------------------------------------------

    reviews_df = spark.read.parquet(
        str(SILVER_ORDER_REVIEWS_PATH)
    )

    # ---------------------------------------------------------
    # Basic information
    # ---------------------------------------------------------

    row_count = reviews_df.count()

    print("✓ Silver order reviews loaded")
    print(f"✓ Rows: {row_count}")
    print(f"✓ Columns: {len(reviews_df.columns)}")

    # ---------------------------------------------------------
    # Expected columns
    # ---------------------------------------------------------

    expected_columns = [
        "review_id",
        "order_id",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_creation_date",
        "review_answer_timestamp",
    ]

    assert reviews_df.columns == expected_columns, (
        f"Unexpected columns.\n"
        f"Expected: {expected_columns}\n"
        f"Actual: {reviews_df.columns}"
    )

    print("✓ Columns are correct")

    # ---------------------------------------------------------
    # Validate review_id
    # ---------------------------------------------------------

    null_review_ids = reviews_df.filter(
        F.col("review_id").isNull()
    ).count()

    assert null_review_ids == 0, (
        f"Found {null_review_ids} NULL review_id values"
    )

    print("✓ review_id contains no NULL values")

    # ---------------------------------------------------------
    # Validate order_id
    # ---------------------------------------------------------

    null_order_ids = reviews_df.filter(
        F.col("order_id").isNull()
    ).count()

    assert null_order_ids == 0, (
        f"Found {null_order_ids} NULL order_id values"
    )

    print("✓ order_id contains no NULL values")

    # ---------------------------------------------------------
    # Validate review_score
    # ---------------------------------------------------------

    invalid_scores = reviews_df.filter(
        (F.col("review_score") < 1)
        | (F.col("review_score") > 5)
        | F.col("review_score").isNull()
    ).count()

    assert invalid_scores == 0, (
        f"Found {invalid_scores} invalid review_score values"
    )

    print("✓ review_score values are between 1 and 5")

    # ---------------------------------------------------------
    # Validate duplicate review IDs
    # ---------------------------------------------------------

    duplicate_review_ids = (
        reviews_df
        .groupBy("review_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    assert duplicate_review_ids == 0, (
        f"Found {duplicate_review_ids} duplicate review_id values"
    )

    print("✓ review_id values are unique")

    # ---------------------------------------------------------
    # Validate duplicate review/order combinations
    # ---------------------------------------------------------

    duplicate_review_orders = (
        reviews_df
        .groupBy("review_id", "order_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    assert duplicate_review_orders == 0, (
        f"Found {duplicate_review_orders} duplicate review/order combinations"
    )

    print("✓ Review/order records are unique")

    # ---------------------------------------------------------
    # Validate timestamp columns
    # ---------------------------------------------------------

    creation_type = dict(
        reviews_df.dtypes
    )["review_creation_date"]

    answer_type = dict(
        reviews_df.dtypes
    )["review_answer_timestamp"]

    assert creation_type == "timestamp", (
        f"review_creation_date has type {creation_type}"
    )

    assert answer_type == "timestamp", (
        f"review_answer_timestamp has type {answer_type}"
    )

    print("✓ review_creation_date is timestamp")
    print("✓ review_answer_timestamp is timestamp")

    # ---------------------------------------------------------
    # Validate review score distribution
    # ---------------------------------------------------------

    print("\nReview score distribution:")

    (
        reviews_df
        .groupBy("review_score")
        .count()
        .orderBy("review_score")
        .show()
    )

    # ---------------------------------------------------------
    # Final result
    # ---------------------------------------------------------

    print("\n" + "=" * 60)
    print("✓ Silver order reviews validation passed")
    print(f"✓ Rows: {row_count}")
    print(f"✓ Columns: {len(reviews_df.columns)}")
    print("✓ review_id contains no NULL values")
    print("✓ order_id contains no NULL values")
    print("✓ review_score values are between 1 and 5")
    print("✓ review_id values are unique")
    print("✓ Review/order records are unique")
    print("✓ Timestamp columns have correct types")
    print("=" * 60)

    spark.stop()