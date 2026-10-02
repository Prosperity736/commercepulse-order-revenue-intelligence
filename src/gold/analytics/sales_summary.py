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

DIM_DATE_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "dim_date"
)

SALES_SUMMARY_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "analytics"
    / "sales_summary"
)


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    spark = create_spark_session()

    print("=" * 80)
    print("BUILDING GOLD ANALYTICS - SALES SUMMARY")
    print("=" * 80)

    # --------------------------------------------------------
    # 1. LOAD GOLD FACT
    # --------------------------------------------------------

    fact_orders = spark.read.parquet(
        str(FACT_ORDERS_PATH)
    )

    print("Gold fact_orders loaded.")
    print(
        f"Fact rows: {fact_orders.count()}"
    )

    # --------------------------------------------------------
    # 2. LOAD DATE DIMENSION
    # --------------------------------------------------------

    dim_date = spark.read.parquet(
        str(DIM_DATE_PATH)
    )

    print("Gold dim_date loaded.")

    # --------------------------------------------------------
    # 3. CREATE ORDER DATE
    # --------------------------------------------------------

    fact_orders = (
        fact_orders
        .withColumn(
            "order_date",
            F.to_date(
                F.col("order_purchase_timestamp")
            )
        )
    )

    # --------------------------------------------------------
    # 4. JOIN DATE DIMENSION
    # --------------------------------------------------------

    sales_df = (
        fact_orders.alias("f")
        .join(
            dim_date.alias("d"),
            F.col("f.order_date") == F.col("d.date"),
            "left"
        )
    )

    print("Date dimension joined.")

    # --------------------------------------------------------
    # 5. BUILD SALES SUMMARY
    # --------------------------------------------------------
    #
    # Grain:
    # One row per order date + order status.
    #
    # Business metrics:
    # - Total orders
    # - Total items
    # - Total item revenue
    # - Total freight
    # - Total order value
    # - Average order value
    # - Average review score
    #

    sales_summary = (
        sales_df
        .groupBy(
            F.col("f.order_date").alias(
                "order_date"
            ),

            F.col("d.date_key").alias(
                "date_key"
            ),

            F.col("d.year").alias(
                "year"
            ),

            F.col("d.quarter").alias(
                "quarter"
            ),

            F.col("d.quarter_name").alias(
                "quarter_name"
            ),

            F.col("d.month").alias(
                "month"
            ),

            F.col("d.month_name").alias(
                "month_name"
            ),

            F.col("d.year_month").alias(
                "year_month"
            ),

            F.col("f.order_status").alias(
                "order_status"
            )
        )
        .agg(

            # Number of unique orders
            F.countDistinct(
                F.col("f.order_id")
            ).alias(
                "total_orders"
            ),

            # Number of order items
            F.count(
                F.col("f.order_item_id")
            ).alias(
                "total_items"
            ),

            # Total product revenue
            F.sum(
                F.col("f.price")
            ).alias(
                "total_item_revenue"
            ),

            # Total shipping/freight cost
            F.sum(
                F.col("f.freight_value")
            ).alias(
                "total_freight"
            ),

            # Total order value
            F.sum(
                F.col("f.total_item_value")
            ).alias(
                "total_order_value"
            ),

            # Average order value
            F.avg(
                F.col("f.total_item_value")
            ).alias(
                "average_order_value"
            ),

            # Average customer review score
            F.avg(
                F.col("f.average_review_score")
            ).alias(
                "average_review_score"
            )
        )
    )

    # --------------------------------------------------------
    # 6. ROUND BUSINESS METRICS
    # --------------------------------------------------------

    sales_summary = (
        sales_summary

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
            "total_order_value",
            F.round(
                F.col("total_order_value"),
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
    # 7. ORDER COLUMNS
    # --------------------------------------------------------

    sales_summary = sales_summary.select(

        "date_key",

        "order_date",

        "year",

        "quarter",

        "quarter_name",

        "month",

        "month_name",

        "year_month",

        "order_status",

        "total_orders",

        "total_items",

        "total_item_revenue",

        "total_freight",

        "total_order_value",

        "average_order_value",

        "average_review_score"
    )

    # --------------------------------------------------------
    # 8. VALIDATION
    # --------------------------------------------------------

    print("\n" + "=" * 80)
    print("SALES SUMMARY VALIDATION")
    print("=" * 80)

    print(
        f"Rows: {sales_summary.count()}"
    )

    print("\nSchema:")

    sales_summary.printSchema()

    print("\nSample records:")

    sales_summary.show(
        10,
        truncate=False
    )

    # --------------------------------------------------------
    # 9. WRITE ANALYTICS TABLE
    # --------------------------------------------------------

    (
        sales_summary.write
        .mode("overwrite")
        .parquet(
            str(SALES_SUMMARY_PATH)
        )
    )

    print(
        "\nSales summary written to:"
    )

    print(
        SALES_SUMMARY_PATH
    )

    # --------------------------------------------------------
    # 10. FINAL VALIDATION
    # --------------------------------------------------------

    print("\n" + "=" * 80)
    print("GOLD SALES SUMMARY COMPLETED SUCCESSFULLY")
    print("=" * 80)

    # --------------------------------------------------------
    # 11. STOP SPARK
    # --------------------------------------------------------

    spark.stop()