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

DIM_SELLERS_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "dim_sellers"
)

SELLER_SUMMARY_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "analytics"
    / "seller_summary"
)


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    spark = create_spark_session()

    print("=" * 80)
    print("BUILDING GOLD ANALYTICS - SELLER SUMMARY")
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
    # 2. LOAD SELLER DIMENSION
    # --------------------------------------------------------

    dim_sellers = spark.read.parquet(
        str(DIM_SELLERS_PATH)
    )

    print("Gold dim_sellers loaded.")

    # --------------------------------------------------------
    # 3. BUILD SELLER METRICS
    # --------------------------------------------------------
    #
    # Grain:
    # One row per seller.
    #
    # Metrics:
    # - Total orders
    # - Total items sold
    # - Unique products sold
    # - Unique customers
    # - Total item revenue
    # - Total freight
    # - Total sales value
    # - Average item price
    # - Average review score
    #

    seller_summary = (
        fact_orders
        .groupBy(
            "seller_id"
        )
        .agg(
            F.countDistinct(
                "order_id"
            ).alias("total_orders"),

            F.count(
                "order_item_id"
            ).alias("total_items"),

            F.countDistinct(
                "product_id"
            ).alias("unique_products_sold"),

            F.countDistinct(
                "customer_unique_id"
            ).alias("unique_customers"),

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
                "price"
            ).alias("average_item_price"),

            F.avg(
                "average_review_score"
            ).alias("average_review_score")
        )
    )

    print("Seller metrics aggregated.")

    # --------------------------------------------------------
    # 4. JOIN SELLER DIMENSION
    # --------------------------------------------------------
    #
    # Adds seller descriptive attributes:
    # - City
    # - State
    # - ZIP code
    #

    seller_summary = (
        seller_summary.alias("s")
        .join(
            dim_sellers.alias("d"),
            F.col("s.seller_id") == F.col("d.seller_id"),
            "left"
        )
        .select(
            F.col("s.seller_id").alias("seller_id"),
            F.col("d.seller_zip_code_prefix").alias(
                "seller_zip_code_prefix"
            ),
            F.col("d.seller_city").alias(
                "seller_city"
            ),
            F.col("d.seller_state").alias(
                "seller_state"
            ),
            F.col("s.total_orders").alias(
                "total_orders"
            ),
            F.col("s.total_items").alias(
                "total_items"
            ),
            F.col("s.unique_products_sold").alias(
                "unique_products_sold"
            ),
            F.col("s.unique_customers").alias(
                "unique_customers"
            ),
            F.col("s.total_item_revenue").alias(
                "total_item_revenue"
            ),
            F.col("s.total_freight").alias(
                "total_freight"
            ),
            F.col("s.total_sales_value").alias(
                "total_sales_value"
            ),
            F.col("s.average_item_price").alias(
                "average_item_price"
            ),
            F.col("s.average_review_score").alias(
                "average_review_score"
            )
        )
    )

    print("Seller dimension joined.")

    # --------------------------------------------------------
    # 5. ROUND BUSINESS METRICS
    # --------------------------------------------------------

    seller_summary = (
        seller_summary
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
    # 6. VALIDATION
    # --------------------------------------------------------

    print("\n" + "=" * 80)
    print("SELLER SUMMARY VALIDATION")
    print("=" * 80)

    print(
        f"Rows: {seller_summary.count()}"
    )

    print("\nSchema:")

    seller_summary.printSchema()

    print("\nSample records:")

    seller_summary.show(
        10,
        truncate=False
    )

    # --------------------------------------------------------
    # 7. DUPLICATE KEY VALIDATION
    # --------------------------------------------------------

    duplicate_sellers = (
        seller_summary
        .groupBy("seller_id")
        .count()
        .filter(
            F.col("count") > 1
        )
    )

    print(
        "Duplicate seller keys:",
        duplicate_sellers.count()
    )

    # --------------------------------------------------------
    # 8. NULL KEY VALIDATION
    # --------------------------------------------------------

    null_seller_keys = (
        seller_summary
        .filter(
            F.col("seller_id").isNull()
        )
        .count()
    )

    print(
        "Null seller keys:",
        null_seller_keys
    )

    # --------------------------------------------------------
    # 9. WRITE ANALYTICS TABLE
    # --------------------------------------------------------

    (
        seller_summary.write
        .mode("overwrite")
        .parquet(
            str(SELLER_SUMMARY_PATH)
        )
    )

    print(
        "\nSeller summary written to:"
    )

    print(
        SELLER_SUMMARY_PATH
    )

    # --------------------------------------------------------
    # 10. FINAL VALIDATION
    # --------------------------------------------------------

    print("\n" + "=" * 80)
    print("GOLD SELLER SUMMARY COMPLETED SUCCESSFULLY")
    print("=" * 80)

    spark.stop()