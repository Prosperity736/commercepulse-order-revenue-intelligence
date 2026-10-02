from pathlib import Path

from pyspark.sql import functions as F

from src.ingestion.spark_session import create_spark_session


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]

FACT_ORDERS_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "fact_orders"
)

CUSTOMER_SUMMARY_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "analytics"
    / "customer_summary"
)


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    spark = create_spark_session()

    print("=" * 80)
    print("BUILDING GOLD ANALYTICS - CUSTOMER SUMMARY")
    print("=" * 80)

    # --------------------------------------------------------
    # 1. LOAD GOLD FACT
    # --------------------------------------------------------

    fact_orders = spark.read.parquet(
        str(FACT_ORDERS_PATH)
    )

    print("Gold fact_orders loaded.")
    print(f"Fact rows: {fact_orders.count()}")

    # --------------------------------------------------------
    # 2. BUILD CUSTOMER SUMMARY
    # --------------------------------------------------------
    #
    # Grain:
    # One row per customer_unique_id.
    #
    # fact_orders is at order-item grain.
    # Therefore, customer-level metrics are calculated
    # by aggregating all order-item records belonging
    # to each customer.
    #

    customer_summary = (
        fact_orders
        .groupBy(
            "customer_unique_id"
        )
        .agg(

            # Number of unique orders
            F.countDistinct(
                "order_id"
            ).alias("total_orders"),

            # Each fact row represents one order item
            F.count(
                "product_id"
            ).alias("total_items"),

            # Product revenue
            F.sum(
                "price"
            ).alias("total_item_revenue"),

            # Freight/shipping cost
            F.sum(
                "freight_value"
            ).alias("total_freight"),

            # Product value + freight
            F.sum(
                "total_item_value"
            ).alias("total_spend"),

            # Average review score
            F.avg(
                "average_review_score"
            ).alias("average_review_score"),

            # First order
            F.min(
                "order_purchase_timestamp"
            ).alias("first_order_date"),

            # Most recent order
            F.max(
                "order_purchase_timestamp"
            ).alias("last_order_date")
        )
    )

    # --------------------------------------------------------
    # 3. CALCULATE TRUE AVERAGE ORDER VALUE
    # --------------------------------------------------------
    #
    # AOV = total customer spend / total customer orders
    #
    # This is calculated AFTER customer aggregation because
    # fact_orders is at order-item grain.
    #

    customer_summary = (
        customer_summary
        .withColumn(
            "average_order_value",
            F.when(
                F.col("total_orders") > 0,
                F.col("total_spend")
                / F.col("total_orders")
            )
        )
    )

    # --------------------------------------------------------
    # 4. ROUND BUSINESS METRICS
    # --------------------------------------------------------

    customer_summary = (
        customer_summary

        .withColumn(
            "total_item_revenue",
            F.round(
                F.col("total_item_revenue"),
                2
            )
        )

        .withColumn(
            "total_freight",
            F.round(
                F.col("total_freight"),
                2
            )
        )

        .withColumn(
            "total_spend",
            F.round(
                F.col("total_spend"),
                2
            )
        )

        .withColumn(
            "average_order_value",
            F.round(
                F.col("average_order_value"),
                2
            )
        )

        .withColumn(
            "average_review_score",
            F.round(
                F.col("average_review_score"),
                2
            )
        )
    )

    # --------------------------------------------------------
    # 5. ORDER COLUMNS
    # --------------------------------------------------------

    customer_summary = customer_summary.select(
        "customer_unique_id",
        "total_orders",
        "total_items",
        "total_item_revenue",
        "total_freight",
        "total_spend",
        "average_order_value",
        "average_review_score",
        "first_order_date",
        "last_order_date"
    )

    # --------------------------------------------------------
    # 6. VALIDATION
    # --------------------------------------------------------

    print("\n" + "=" * 80)
    print("CUSTOMER SUMMARY VALIDATION")
    print("=" * 80)

    print(
        f"Rows: {customer_summary.count()}"
    )

    print("\nSchema:")

    customer_summary.printSchema()

    print("\nSample records:")

    customer_summary.show(
        10,
        truncate=False
    )

    # --------------------------------------------------------
    # 7. VALIDATE CUSTOMER GRAIN
    # --------------------------------------------------------

    duplicate_customers = (
        customer_summary
        .groupBy(
            "customer_unique_id"
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    null_customers = (
        customer_summary
        .filter(
            F.col("customer_unique_id").isNull()
        )
        .count()
    )

    print(
        f"Duplicate customer keys: {duplicate_customers}"
    )

    print(
        f"Null customer keys: {null_customers}"
    )

    # --------------------------------------------------------
    # 8. WRITE ANALYTICS TABLE
    # --------------------------------------------------------

    (
        customer_summary.write
        .mode("overwrite")
        .parquet(
            str(CUSTOMER_SUMMARY_PATH)
        )
    )

    print(
        "\nCustomer summary written to:"
    )

    print(
        CUSTOMER_SUMMARY_PATH
    )

    # --------------------------------------------------------
    # 9. FINAL VALIDATION
    # --------------------------------------------------------

    print("\n" + "=" * 80)
    print(
        "GOLD CUSTOMER SUMMARY COMPLETED SUCCESSFULLY"
    )
    print("=" * 80)

    spark.stop()