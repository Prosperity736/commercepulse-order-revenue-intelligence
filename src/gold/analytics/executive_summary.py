from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# =============================================================================
# SPARK SESSION
# =============================================================================

spark = (
    SparkSession.builder
    .appName("Gold Executive Summary")
    .getOrCreate()
)

print("=" * 80)
print("BUILDING GOLD ANALYTICS - EXECUTIVE SUMMARY")
print("=" * 80)


# =============================================================================
# PATHS
# =============================================================================

FACT_ORDERS_PATH = "data/gold/fact_orders"
CUSTOMER_SUMMARY_PATH = "data/gold/analytics/customer_summary"
PRODUCT_SUMMARY_PATH = "data/gold/analytics/product_summary"
SELLER_SUMMARY_PATH = "data/gold/analytics/seller_summary"
PAYMENT_SUMMARY_PATH = "data/gold/analytics/payment_summary"
DELIVERY_SUMMARY_PATH = "data/gold/analytics/delivery_summary"

OUTPUT_PATH = "data/gold/analytics/executive_summary"


# =============================================================================
# LOAD GOLD TABLES
# =============================================================================

fact_orders = spark.read.parquet(FACT_ORDERS_PATH)
customer_summary = spark.read.parquet(CUSTOMER_SUMMARY_PATH)
product_summary = spark.read.parquet(PRODUCT_SUMMARY_PATH)
seller_summary = spark.read.parquet(SELLER_SUMMARY_PATH)
payment_summary = spark.read.parquet(PAYMENT_SUMMARY_PATH)
delivery_summary = spark.read.parquet(DELIVERY_SUMMARY_PATH)

print("Gold analytics tables loaded.")


# =============================================================================
# EXECUTIVE KPIs
# =============================================================================

# -------------------------------------------------------------------------
# Order / sales KPIs
# -------------------------------------------------------------------------

sales_metrics = fact_orders.agg(
    F.countDistinct("order_id").alias("total_orders"),
    F.sum("order_item_id").alias("total_items"),
    F.sum("item_revenue").alias("total_item_revenue"),
    F.sum("freight_value").alias("total_freight"),
    F.sum("total_item_value").alias("total_sales_value"),
    F.avg("price").alias("average_item_price"),
    F.avg("average_review_score").alias("average_review_score"),
    F.countDistinct("customer_unique_id").alias("total_customers"),
    F.countDistinct("product_id").alias("total_products"),
    F.countDistinct("seller_id").alias("total_sellers"),
)


# -------------------------------------------------------------------------
# Payment KPIs
# -------------------------------------------------------------------------

payment_metrics = payment_summary.agg(
    F.sum("total_payment_value").alias("total_payment_value")
)


# -------------------------------------------------------------------------
# Delivery KPIs
# -------------------------------------------------------------------------

delivery_metrics = delivery_summary.agg(
    F.sum("delivered_orders").alias("delivered_orders"),
    F.sum("canceled_orders").alias("canceled_orders"),
    F.sum("shipped_orders").alias("shipped_orders"),
    F.sum("late_deliveries").alias("late_deliveries"),
    F.sum("on_time_deliveries").alias("on_time_deliveries"),
)


# =============================================================================
# COMBINE ALL KPIs
# =============================================================================

executive_summary = (
    sales_metrics
    .crossJoin(payment_metrics)
    .crossJoin(delivery_metrics)
)


# =============================================================================
# CALCULATED EXECUTIVE METRICS
# =============================================================================

executive_summary = (
    executive_summary

    # Average order value
    .withColumn(
        "average_order_value",
        F.when(
            F.col("total_orders") > 0,
            F.col("total_sales_value") / F.col("total_orders")
        )
    )

    # On-time delivery rate
    .withColumn(
        "on_time_delivery_rate",
        F.when(
            (F.col("on_time_deliveries") + F.col("late_deliveries")) > 0,
            (
                F.col("on_time_deliveries")
                / (
                    F.col("on_time_deliveries")
                    + F.col("late_deliveries")
                )
                * 100
            )
        )
    )

    # Round financial metrics
    .withColumn(
        "total_item_revenue",
        F.round("total_item_revenue", 2)
    )
    .withColumn(
        "total_freight",
        F.round("total_freight", 2)
    )
    .withColumn(
        "total_sales_value",
        F.round("total_sales_value", 2)
    )
    .withColumn(
        "total_payment_value",
        F.round("total_payment_value", 2)
    )
    .withColumn(
        "average_item_price",
        F.round("average_item_price", 2)
    )
    .withColumn(
        "average_order_value",
        F.round("average_order_value", 2)
    )
    .withColumn(
        "average_review_score",
        F.round("average_review_score", 2)
    )
    .withColumn(
        "on_time_delivery_rate",
        F.round("on_time_delivery_rate", 2)
    )
)


# =============================================================================
# REORDER COLUMNS
# =============================================================================

executive_summary = executive_summary.select(
    "total_orders",
    "total_items",
    "total_customers",
    "total_products",
    "total_sellers",
    "total_item_revenue",
    "total_freight",
    "total_sales_value",
    "total_payment_value",
    "average_item_price",
    "average_order_value",
    "average_review_score",
    "delivered_orders",
    "shipped_orders",
    "canceled_orders",
    "late_deliveries",
    "on_time_deliveries",
    "on_time_delivery_rate",
)


# =============================================================================
# VALIDATION
# =============================================================================

print()
print("=" * 80)
print("EXECUTIVE SUMMARY VALIDATION")
print("=" * 80)

print(f"Rows: {executive_summary.count()}")

print()
print("Schema:")
executive_summary.printSchema()

print()
print("Executive KPIs:")
executive_summary.show(truncate=False)


# =============================================================================
# VALIDATION RULES
# =============================================================================

row_count = executive_summary.count()

if row_count != 1:
    raise ValueError(
        f"Executive summary should contain exactly 1 row, found {row_count}"
    )

null_key_columns = [
    "total_orders",
    "total_customers",
    "total_products",
    "total_sellers",
]

for column_name in null_key_columns:

    null_count = (
        executive_summary
        .filter(F.col(column_name).isNull())
        .count()
    )

    if null_count > 0:
        raise ValueError(
            f"Null values found in {column_name}: {null_count}"
        )

print()
print("Executive summary validation passed.")


# =============================================================================
# WRITE GOLD TABLE
# =============================================================================

(
    executive_summary
    .write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)

print()
print("Executive summary written to:")
print(OUTPUT_PATH)

print()
print("=" * 80)
print("GOLD EXECUTIVE SUMMARY COMPLETED SUCCESSFULLY")
print("=" * 80)


# =============================================================================
# STOP SPARK
# =============================================================================

spark.stop()