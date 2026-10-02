from pathlib import Path

from pyspark.sql import functions as F

from src.ingestion.spark_session import create_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_ORDER_PAYMENTS_PATH = (
    PROJECT_ROOT
    / "data"
    / "bronze"
    / "order_payments"
)

SILVER_ORDER_PAYMENTS_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "order_payments"
)


if __name__ == "__main__":

    spark = create_spark_session()

    # --------------------------------------------------------
    # 1. READ BRONZE ORDER PAYMENTS
    # --------------------------------------------------------

    payments_df = spark.read.parquet(
        str(BRONZE_ORDER_PAYMENTS_PATH)
    )

    print("Bronze order payments loaded.")
    print(
        f"Bronze rows: {payments_df.count()}"
    )

    print("\nBronze schema:")
    payments_df.printSchema()


    # --------------------------------------------------------
    # 2. CLEAN AND CAST PAYMENT COLUMNS
    # --------------------------------------------------------

    silver_payments_df = (
        payments_df

        # Order ID
        .withColumn(
            "order_id",
            F.trim(F.col("order_id"))
        )

        # Payment sequential
        .withColumn(
            "payment_sequential",
            F.expr(
                "try_cast(trim(payment_sequential) AS INT)"
            )
        )

        # Payment type
        .withColumn(
            "payment_type",
            F.trim(
                F.lower(
                    F.col("payment_type")
                )
            )
        )

        # Payment installments
        .withColumn(
            "payment_installments",
            F.expr(
                "try_cast(trim(payment_installments) AS INT)"
            )
        )

        # Payment value
        .withColumn(
            "payment_value",
            F.expr(
                "try_cast(trim(payment_value) AS DOUBLE)"
            )
        )
    )


    # --------------------------------------------------------
    # 3. HANDLE NULL PAYMENT VALUES
    # --------------------------------------------------------

    silver_payments_df = (
        silver_payments_df

        .withColumn(
            "payment_installments",
            F.coalesce(
                F.col("payment_installments"),
                F.lit(0)
            )
        )

        .withColumn(
            "payment_value",
            F.coalesce(
                F.col("payment_value"),
                F.lit(0.0)
            )
        )
    )


    # --------------------------------------------------------
    # 4. VALIDATE PAYMENT VALUES
    # --------------------------------------------------------

    silver_payments_df = (
        silver_payments_df

        .filter(
            F.col("payment_installments") >= 0
        )

        .filter(
            F.col("payment_value") >= 0
        )
    )


    # --------------------------------------------------------
    # 5. REMOVE INVALID ORDER IDs
    # --------------------------------------------------------

    silver_payments_df = (
        silver_payments_df
        .filter(
            F.col("order_id").isNotNull()
        )
    )


    # --------------------------------------------------------
    # 6. REMOVE DUPLICATE PAYMENTS
    # --------------------------------------------------------

    silver_payments_df = (
        silver_payments_df
        .dropDuplicates(
            [
                "order_id",
                "payment_sequential"
            ]
        )
    )


    # --------------------------------------------------------
    # 7. SELECT FINAL SILVER COLUMNS
    # --------------------------------------------------------

    silver_payments_df = silver_payments_df.select(
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value"
    )


    # --------------------------------------------------------
    # 8. DISPLAY TRANSFORMATION RESULTS
    # --------------------------------------------------------

    print(
        "\nSilver order payments transformation completed!"
    )

    print(
        f"Number of rows: "
        f"{silver_payments_df.count()}"
    )

    print("\nSilver schema:")
    silver_payments_df.printSchema()

    print("\nSample Silver records:")

    silver_payments_df.show(
        5,
        truncate=False
    )


    # --------------------------------------------------------
    # 9. WRITE SILVER ORDER PAYMENTS
    # --------------------------------------------------------

    (
        silver_payments_df.write
        .mode("overwrite")
        .parquet(
            str(SILVER_ORDER_PAYMENTS_PATH)
        )
    )


    print(
        f"\nSilver order payments written to: "
        f"{SILVER_ORDER_PAYMENTS_PATH}"
    )


    # --------------------------------------------------------
    # 10. STOP SPARK
    # --------------------------------------------------------

    spark.stop()