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

GEOGRAPHY_SUMMARY_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "analytics"
    / "geography_summary"
)


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    spark = create_spark_session()

    print("=" * 80)
    print("BUILDING GOLD ANALYTICS - GEOGRAPHY SUMMARY")
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
    # 2. BUILD GEOGRAPHY METRICS
    # --------------------------------------------------------
    #
    # Grain:
    # One row per customer state.
    #
    # This table helps analyze:
    # - Order activity by state
    # - Customer distribution
    # - Product activity
    # - Revenue
    # - Freight
    # - Average order value
    # - Customer satisfaction
    #

    geography_summary = (
        fact_orders
        .groupBy(
            "customer_state"
        )
        .agg(
            F.countDistinct(
                "order_id"
            ).alias("total_orders"),

            F.count(
                "order_item_id"
            ).alias("total_items"),

            F.countDistinct(
                "customer_unique_id"
            ).alias("unique_customers"),

            F.countDistinct(
                "product_id"
            ).alias("unique_products"),

            F.sum(
                "price"
            ).alias("total_item_revenue"),

            F.sum(
                "freight_value"
            ).alias("total_freight"),

            F.sum(
                "total_item_value"
            ).alias("total_sales_value"),

            F.avg(
                "total_item_value"
            ).alias("average_order_value"),

            F.avg(
                "average_review_score"
            ).alias("average_review_score")
        )
    )

    print("Geography metrics aggregated.")

    # --------------------------------------------------------
    # 3. ROUND BUSINESS METRICS
    # --------------------------------------------------------

    geography_summary = (
        geography_summary
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
            "total_sales_value",
            F.round(
                F.col("total_sales_value"),
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
    # 4. ORDER COLUMNS
    # --------------------------------------------------------

    geography_summary = geography_summary.select(
        "customer_state",
        "total_orders",
        "total_items",
        "unique_customers",
        "unique_products",
        "total_item_revenue",
        "total_freight",
        "total_sales_value",
        "average_order_value",
        "average_review_score"
    )

    # --------------------------------------------------------
    # 5. VALIDATION
    # --------------------------------------------------------

    print("\n" + "=" * 80)
    print("GEOGRAPHY SUMMARY VALIDATION")
    print("=" * 80)

    print(
        f"Rows: {geography_summary.count()}"
    )

    print("\nSchema:")

    geography_summary.printSchema()

    print("\nSample records:")

    geography_summary.show(
        10,
        truncate=False
    )

    # --------------------------------------------------------
    # 6. DUPLICATE KEY VALIDATION
    # --------------------------------------------------------

    duplicate_states = (
        geography_summary
        .groupBy("customer_state")
        .count()
        .filter(
            F.col("count") > 1
        )
    )

    print(
        "Duplicate customer_state keys:",
        duplicate_states.count()
    )

    # --------------------------------------------------------
    # 7. NULL KEY VALIDATION
    # --------------------------------------------------------

    null_states = (
        geography_summary
        .filter(
            F.col("customer_state").isNull()
        )
        .count()
    )

    print(
        "Null customer_state keys:",
        null_states
    )

    # --------------------------------------------------------
    # 8. WRITE ANALYTICS TABLE
    # --------------------------------------------------------

    (
        geography_summary.write
        .mode("overwrite")
        .parquet(
            str(GEOGRAPHY_SUMMARY_PATH)
        )
    )

    print(
        "\nGeography summary written to:"
    )

    print(
        GEOGRAPHY_SUMMARY_PATH
    )

    # --------------------------------------------------------
    # 9. FINAL VALIDATION
    # --------------------------------------------------------

    print("\n" + "=" * 80)
    print("GOLD GEOGRAPHY SUMMARY COMPLETED SUCCESSFULLY")
    print("=" * 80)

    spark.stop()