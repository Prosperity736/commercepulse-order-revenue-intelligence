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

PRODUCT_SUMMARY_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "analytics"
    / "product_summary"
)


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    spark = create_spark_session()

    print("=" * 80)
    print("BUILDING GOLD ANALYTICS - PRODUCT SUMMARY")
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
    # 2. BUILD PRODUCT SUMMARY
    # --------------------------------------------------------
    #
    # Grain:
    # One row per product.
    #
    # Because fact_orders is at order-item grain:
    # - countDistinct(order_id) = number of orders
    # - count(order_item_id) = number of items sold
    #
    # --------------------------------------------------------

    product_summary = (
        fact_orders
        .groupBy(
            F.col("product_id"),
            F.col("product_category_name"),
            F.col("product_category_name_english")
        )
        .agg(

            F.countDistinct(
                F.col("order_id")
            ).alias("total_orders"),

            F.count(
                F.col("order_item_id")
            ).alias("total_items"),

            F.sum(
                F.col("price")
            ).alias("total_item_revenue"),

            F.sum(
                F.col("freight_value")
            ).alias("total_freight"),

            F.sum(
                F.col("total_item_value")
            ).alias("total_sales_value"),

            F.avg(
                F.col("price")
            ).alias("average_item_price"),

            F.avg(
                F.col("average_review_score")
            ).alias("average_review_score")
        )
    )

    print("Product metrics aggregated.")

    # --------------------------------------------------------
    # 3. ROUND BUSINESS METRICS
    # --------------------------------------------------------

    product_summary = (
        product_summary

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
            "average_item_price",
            F.round(
                F.col("average_item_price"),
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

    product_summary = product_summary.select(
        "product_id",
        "product_category_name",
        "product_category_name_english",
        "total_orders",
        "total_items",
        "total_item_revenue",
        "total_freight",
        "total_sales_value",
        "average_item_price",
        "average_review_score"
    )

    # --------------------------------------------------------
    # 5. VALIDATION
    # --------------------------------------------------------

    print("\n" + "=" * 80)
    print("PRODUCT SUMMARY VALIDATION")
    print("=" * 80)

    print(
        f"Rows: {product_summary.count()}"
    )

    print("\nSchema:")

    product_summary.printSchema()

    print("\nSample records:")

    product_summary.show(
        10,
        truncate=False
    )

    # --------------------------------------------------------
    # 6. KEY VALIDATION
    # --------------------------------------------------------

    duplicate_products = (
        product_summary
        .groupBy("product_id")
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    null_product_ids = (
        product_summary
        .filter(
            F.col("product_id").isNull()
        )
        .count()
    )

    print(
        f"Duplicate product keys: {duplicate_products}"
    )

    print(
        f"Null product keys: {null_product_ids}"
    )

    # --------------------------------------------------------
    # 7. WRITE ANALYTICS TABLE
    # --------------------------------------------------------

    (
        product_summary.write
        .mode("overwrite")
        .parquet(
            str(PRODUCT_SUMMARY_PATH)
        )
    )

    print(
        "\nProduct summary written to:"
    )

    print(PRODUCT_SUMMARY_PATH)

    # --------------------------------------------------------
    # 8. FINAL VALIDATION
    # --------------------------------------------------------

    print("\n" + "=" * 80)
    print("GOLD PRODUCT SUMMARY COMPLETED SUCCESSFULLY")
    print("=" * 80)

    spark.stop()
    