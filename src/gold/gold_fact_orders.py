from pathlib import Path

from pyspark.sql import functions as F

from src.ingestion.spark_session import create_spark_session


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_DATA_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DATA_DIR = PROJECT_ROOT / "data" / "gold"


# ============================================================
# SILVER DATA PATHS
# ============================================================

SILVER_ORDERS_PATH = SILVER_DATA_DIR / "orders"
SILVER_ORDER_ITEMS_PATH = SILVER_DATA_DIR / "order_items"
SILVER_ORDER_PAYMENTS_PATH = SILVER_DATA_DIR / "order_payments"
SILVER_ORDER_REVIEWS_PATH = SILVER_DATA_DIR / "order_reviews"
SILVER_CUSTOMERS_PATH = SILVER_DATA_DIR / "customers"
SILVER_PRODUCTS_PATH = SILVER_DATA_DIR / "products"
SILVER_SELLERS_PATH = SILVER_DATA_DIR / "sellers"

SILVER_CATEGORY_TRANSLATION_PATH = (
    SILVER_DATA_DIR / "product_category_translation"
)


# ============================================================
# GOLD OUTPUT
# ============================================================

GOLD_FACT_ORDERS_PATH = GOLD_DATA_DIR / "fact_orders"


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    spark = create_spark_session()

    print("=" * 80)
    print("BUILDING GOLD FACT ORDERS")
    print("=" * 80)

    # --------------------------------------------------------
    # 1. READ SILVER DATA
    # --------------------------------------------------------

    orders_df = spark.read.parquet(
        str(SILVER_ORDERS_PATH)
    )

    order_items_df = spark.read.parquet(
        str(SILVER_ORDER_ITEMS_PATH)
    )

    payments_df = spark.read.parquet(
        str(SILVER_ORDER_PAYMENTS_PATH)
    )

    reviews_df = spark.read.parquet(
        str(SILVER_ORDER_REVIEWS_PATH)
    )

    customers_df = spark.read.parquet(
        str(SILVER_CUSTOMERS_PATH)
    )

    products_df = spark.read.parquet(
        str(SILVER_PRODUCTS_PATH)
    )

    sellers_df = spark.read.parquet(
        str(SILVER_SELLERS_PATH)
    )

    category_translation_df = spark.read.parquet(
        str(SILVER_CATEGORY_TRANSLATION_PATH)
    )

    print("\nSilver datasets loaded successfully.")

    # --------------------------------------------------------
    # 2. AGGREGATE PAYMENTS TO ORDER LEVEL
    # --------------------------------------------------------

    payments_agg_df = (
        payments_df
        .groupBy("order_id")
        .agg(
            F.sum("payment_value").alias(
                "total_payment_value"
            ),
            F.count("*").alias(
                "payment_count"
            ),
            F.max("payment_installments").alias(
                "max_payment_installments"
            )
        )
    )

    print("Payments aggregated to order level.")

    # --------------------------------------------------------
    # 3. AGGREGATE REVIEWS TO ORDER LEVEL
    # --------------------------------------------------------

    reviews_agg_df = (
        reviews_df
        .groupBy("order_id")
        .agg(
            F.avg("review_score").alias(
                "average_review_score"
            ),
            F.count("*").alias(
                "review_count"
            )
        )
    )

    print("Reviews aggregated to order level.")

    # --------------------------------------------------------
    # 4. JOIN ORDERS WITH ORDER ITEMS
    # --------------------------------------------------------

    fact_orders_df = (
        order_items_df
        .join(
            orders_df,
            on="order_id",
            how="left"
        )
    )

    print("Orders joined with order items.")

    # --------------------------------------------------------
    # 5. JOIN CUSTOMER
    # --------------------------------------------------------

    fact_orders_df = (
        fact_orders_df
        .join(
            customers_df,
            on="customer_id",
            how="left"
        )
    )

    print("Customers joined.")

    # --------------------------------------------------------
    # 6. JOIN PRODUCTS
    # --------------------------------------------------------

    fact_orders_df = (
        fact_orders_df
        .join(
            products_df,
            on="product_id",
            how="left"
        )
    )

    print("Products joined.")

    # --------------------------------------------------------
    # 7. JOIN PRODUCT CATEGORY TRANSLATION
    # --------------------------------------------------------

    fact_orders_df = (
        fact_orders_df
        .join(
            category_translation_df,
            on="product_category_name",
            how="left"
        )
    )

    print("Product category translation joined.")

    # --------------------------------------------------------
    # 8. JOIN SELLERS
    # --------------------------------------------------------

    fact_orders_df = (
        fact_orders_df
        .join(
            sellers_df,
            on="seller_id",
            how="left"
        )
    )

    print("Sellers joined.")

    # --------------------------------------------------------
    # 9. JOIN AGGREGATED PAYMENTS
    # --------------------------------------------------------

    fact_orders_df = (
        fact_orders_df
        .join(
            payments_agg_df,
            on="order_id",
            how="left"
        )
    )

    print("Aggregated payments joined.")

    # --------------------------------------------------------
    # 10. JOIN AGGREGATED REVIEWS
    # --------------------------------------------------------

    fact_orders_df = (
        fact_orders_df
        .join(
            reviews_agg_df,
            on="order_id",
            how="left"
        )
    )

    print("Aggregated reviews joined.")

    # --------------------------------------------------------
    # 11. CREATE BUSINESS METRICS
    # --------------------------------------------------------

    fact_orders_df = (
        fact_orders_df

        # Item revenue
        .withColumn(
            "item_revenue",
            F.col("price")
        )

        # Total item value including freight
        .withColumn(
            "total_item_value",
            F.col("price") + F.col("freight_value")
        )

        # Purchase date for the Date dimension relationship
        .withColumn(
            "purchase_date",
            F.to_date(
                F.col("order_purchase_timestamp")
            )
        )

        # Number of days from purchase to delivery
        .withColumn(
            "delivery_days",
            F.when(
                F.col(
                    "order_delivered_customer_date"
                ).isNotNull()
                &
                F.col(
                    "order_purchase_timestamp"
                ).isNotNull(),
                F.datediff(
                    F.to_date(
                        F.col(
                            "order_delivered_customer_date"
                        )
                    ),
                    F.to_date(
                        F.col(
                            "order_purchase_timestamp"
                        )
                    )
                )
            )
        )

        # Difference between actual delivery date
        # and estimated delivery date
        .withColumn(
            "delivery_delay_days",
            F.when(
                F.col(
                    "order_delivered_customer_date"
                ).isNotNull()
                &
                F.col(
                    "order_estimated_delivery_date"
                ).isNotNull(),
                F.datediff(
                    F.to_date(
                        F.col(
                            "order_delivered_customer_date"
                        )
                    ),
                    F.to_date(
                        F.col(
                            "order_estimated_delivery_date"
                        )
                    )
                )
            )
        )
    )

    # --------------------------------------------------------
    # 12. SELECT ANALYTICAL COLUMNS
    # --------------------------------------------------------

    fact_orders_df = fact_orders_df.select(

        # Order grain
        "order_id",
        "order_item_id",

        # Customer
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state",

        # Order
        "order_status",
        "order_purchase_timestamp",
        "purchase_date",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",

        # Product
        "product_id",
        "product_category_name",
        "product_category_name_english",

        # Seller
        "seller_id",
        "seller_city",
        "seller_state",

        # Financial
        "price",
        "freight_value",
        "item_revenue",
        "total_item_value",

        # Payments
        "total_payment_value",
        "payment_count",
        "max_payment_installments",

        # Reviews
        "average_review_score",
        "review_count",

        # Delivery
        "delivery_days",
        "delivery_delay_days",

        # Product attributes
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "product_photos_qty"
    )

    # --------------------------------------------------------
    # 13. WRITE GOLD DATA
    # --------------------------------------------------------

    (
        fact_orders_df
        .write
        .mode("overwrite")
        .parquet(
            str(GOLD_FACT_ORDERS_PATH)
        )
    )

    print("\nGold fact_orders written to:")
    print(GOLD_FACT_ORDERS_PATH)

    # --------------------------------------------------------
    # 14. VALIDATION
    # --------------------------------------------------------

    print("\n" + "=" * 80)
    print("GOLD FACT ORDERS VALIDATION")
    print("=" * 80)

    print(
        f"Rows: {fact_orders_df.count()}"
    )

    print("\nSchema:")

    fact_orders_df.printSchema()

    print("\nSample records:")

    fact_orders_df.show(
        5,
        truncate=False
    )

    print("=" * 80)
    print("GOLD FACT ORDERS COMPLETED SUCCESSFULLY")
    print("=" * 80)

    spark.stop()