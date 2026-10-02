from pathlib import Path

from pyspark.sql import functions as F

from src.ingestion.spark_session import create_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SILVER_ORDER_PAYMENTS_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "order_payments"
)


def main():

    spark = create_spark_session()

    payments_df = spark.read.parquet(
        str(SILVER_ORDER_PAYMENTS_PATH)
    )

    # Basic validation
    row_count = payments_df.count()
    column_count = len(payments_df.columns)

    assert row_count > 0, "Silver order payments contains no rows"

    assert column_count == 5, (
        f"Expected 5 columns, found {column_count}"
    )

    # order_id should not be NULL
    null_order_ids = payments_df.filter(
        F.col("order_id").isNull()
    ).count()

    assert null_order_ids == 0, (
        f"Found {null_order_ids} NULL order_id values"
    )

    # Payment sequence should not be negative
    negative_sequences = payments_df.filter(
        F.col("payment_sequential") < 0
    ).count()

    assert negative_sequences == 0, (
        f"Found {negative_sequences} negative payment sequences"
    )

    # Installments should not be negative
    negative_installments = payments_df.filter(
        F.col("payment_installments") < 0
    ).count()

    assert negative_installments == 0, (
        f"Found {negative_installments} negative installments"
    )

    # Payment value should not be negative
    negative_values = payments_df.filter(
        F.col("payment_value") < 0
    ).count()

    assert negative_values == 0, (
        f"Found {negative_values} negative payment values"
    )

    # Validate payment uniqueness
    duplicate_payments = (
        payments_df
        .groupBy(
            "order_id",
            "payment_sequential"
        )
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    assert duplicate_payments == 0, (
        f"Found {duplicate_payments} duplicate payment records"
    )

    print("✓ Silver order payments validation passed")
    print(f"✓ Rows: {row_count}")
    print(f"✓ Columns: {column_count}")
    print("✓ order_id contains no NULL values")
    print("✓ No negative payment sequences")
    print("✓ No negative installments")
    print("✓ No negative payment values")
    print("✓ Payment records are unique")

    spark.stop()


if __name__ == "__main__":
    main()
    