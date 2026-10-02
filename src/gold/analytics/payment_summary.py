from pathlib import Path

from pyspark.sql import functions as F

from src.ingestion.spark_session import create_spark_session


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]

SILVER_DATA_DIR = (
    PROJECT_ROOT
    / "data"
    / "silver"
)

SILVER_ORDER_PAYMENTS_PATH = (
    SILVER_DATA_DIR
    / "order_payments"
)

SILVER_ORDERS_PATH = (
    SILVER_DATA_DIR
    / "orders"
)

PAYMENT_SUMMARY_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "analytics"
    / "payment_summary"
)


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    spark = create_spark_session()

    print("=" * 80)
    print("BUILDING GOLD ANALYTICS - PAYMENT SUMMARY")
    print("=" * 80)

    # --------------------------------------------------------
    # 1. LOAD SILVER PAYMENT DATA
    # --------------------------------------------------------

    payments_df = spark.read.parquet(
        str(SILVER_ORDER_PAYMENTS_PATH)
    )

    print("Silver order_payments loaded.")
    print(
        f"Payment rows: {payments_df.count()}"
    )

    # --------------------------------------------------------
    # 2. LOAD SILVER ORDERS
    # --------------------------------------------------------
    #
    # We need orders only to connect payment records
    # to customer_unique_id.
    #
    # This allows us to calculate:
    # - unique customers
    # - customer payment activity
    #
    # We are NOT joining this into fact_orders.
    #

    orders_df = (
        spark.read.parquet(
            str(SILVER_ORDERS_PATH)
        )
        .select(
            "order_id",
            "customer_id"
        )
    )

    print("Silver orders loaded.")

    # --------------------------------------------------------
    # 3. LOAD CUSTOMER DATA
    # --------------------------------------------------------

    customers_df = (
        spark.read.parquet(
            str(
                PROJECT_ROOT
                / "data"
                / "silver"
                / "customers"
            )
        )
        .select(
            "customer_id",
            "customer_unique_id"
        )
    )

    print("Silver customers loaded.")

    # --------------------------------------------------------
    # 4. CONNECT PAYMENTS TO CUSTOMERS
    # --------------------------------------------------------
    #
    # Payment data contains order_id.
    #
    # orders gives us:
    # order_id → customer_id
    #
    # customers gives us:
    # customer_id → customer_unique_id
    #
    # Therefore:
    #
    # payment
    #    ↓
    # order
    #    ↓
    # customer
    #

    payment_customer_df = (
        payments_df
        .join(
            orders_df,
            on="order_id",
            how="left"
        )
        .join(
            customers_df,
            on="customer_id",
            how="left"
        )
    )

    print(
        "Payment records connected to customers."
    )

    # --------------------------------------------------------
    # 5. BUILD PAYMENT SUMMARY
    # --------------------------------------------------------
    #
    # Grain:
    #
    # One row per payment_type.
    #
    # Example:
    #
    # credit_card
    # boleto
    # voucher
    # debit_card
    # not_defined
    #
    # This table is designed for:
    #
    # - Payment method analysis
    # - Payment value analysis
    # - Installment analysis
    # - Customer payment behavior
    #

    payment_summary = (
        payment_customer_df
        .groupBy(
            F.col("payment_type")
        )
        .agg(

            # Number of unique orders
            F.countDistinct(
                F.col("order_id")
            ).alias(
                "total_orders"
            ),

            # Number of payment records
            F.count(
                F.col("payment_type")
            ).alias(
                "total_payment_records"
            ),

            # Number of unique customers
            F.countDistinct(
                F.col("customer_unique_id")
            ).alias(
                "unique_customers"
            ),

            # Total payment value
            F.sum(
                F.col("payment_value")
            ).alias(
                "total_payment_value"
            ),

            # Average payment value
            F.avg(
                F.col("payment_value")
            ).alias(
                "average_payment_value"
            ),

            # Average number of installments
            F.avg(
                F.col("payment_installments")
            ).alias(
                "average_installments"
            ),

            # Maximum installments used
            F.max(
                F.col("payment_installments")
            ).alias(
                "max_installments"
            )
        )
    )

    print(
        "Payment metrics aggregated."
    )

    # --------------------------------------------------------
    # 6. ROUND BUSINESS METRICS
    # --------------------------------------------------------

    payment_summary = (
        payment_summary

        .withColumn(
            "total_payment_value",
            F.round(
                F.col("total_payment_value"),
                2
            )
        )

        .withColumn(
            "average_payment_value",
            F.round(
                F.col("average_payment_value"),
                2
            )
        )

        .withColumn(
            "average_installments",
            F.round(
                F.col("average_installments"),
                2
            )
        )
    )

    # --------------------------------------------------------
    # 7. ORDER COLUMNS
    # --------------------------------------------------------

    payment_summary = payment_summary.select(
        "payment_type",
        "total_orders",
        "total_payment_records",
        "unique_customers",
        "total_payment_value",
        "average_payment_value",
        "average_installments",
        "max_installments"
    )

    # --------------------------------------------------------
    # 8. VALIDATION
    # --------------------------------------------------------

    print("\n" + "=" * 80)
    print("PAYMENT SUMMARY VALIDATION")
    print("=" * 80)

    print(
        f"Rows: {payment_summary.count()}"
    )

    print("\nSchema:")

    payment_summary.printSchema()

    print("\nPayment summary:")

    payment_summary.show(
        20,
        truncate=False
    )

    # --------------------------------------------------------
    # 9. KEY VALIDATION
    # --------------------------------------------------------

    duplicate_payment_type_keys = (
        payment_summary
        .groupBy(
            "payment_type"
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    null_payment_type_keys = (
        payment_summary
        .filter(
            F.col("payment_type").isNull()
        )
        .count()
    )

    print(
        f"Duplicate payment_type keys: "
        f"{duplicate_payment_type_keys}"
    )

    print(
        f"Null payment_type keys: "
        f"{null_payment_type_keys}"
    )

    # --------------------------------------------------------
    # 10. WRITE ANALYTICS TABLE
    # --------------------------------------------------------

    (
        payment_summary.write
        .mode("overwrite")
        .parquet(
            str(PAYMENT_SUMMARY_PATH)
        )
    )

    print(
        "\nPayment summary written to:"
    )

    print(PAYMENT_SUMMARY_PATH)

    # --------------------------------------------------------
    # 11. FINAL VALIDATION
    # --------------------------------------------------------

    print("\n" + "=" * 80)
    print("GOLD PAYMENT SUMMARY COMPLETED SUCCESSFULLY")
    print("=" * 80)

    spark.stop()