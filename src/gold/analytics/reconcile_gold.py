from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# =============================================================================
# SPARK SESSION
# =============================================================================

spark = (
    SparkSession.builder
    .appName("Gold Layer Reconciliation")
    .getOrCreate()
)

print("=" * 80)
print("GOLD LAYER RECONCILIATION")
print("=" * 80)


# =============================================================================
# PATHS
# =============================================================================

FACT_ORDERS_PATH = "data/gold/fact_orders"

CUSTOMER_SUMMARY_PATH = (
    "data/gold/analytics/customer_summary"
)

PRODUCT_SUMMARY_PATH = (
    "data/gold/analytics/product_summary"
)

SELLER_SUMMARY_PATH = (
    "data/gold/analytics/seller_summary"
)

PAYMENT_SUMMARY_PATH = (
    "data/gold/analytics/payment_summary"
)

DELIVERY_SUMMARY_PATH = (
    "data/gold/analytics/delivery_summary"
)

EXECUTIVE_SUMMARY_PATH = (
    "data/gold/analytics/executive_summary"
)


# =============================================================================
# LOAD TABLES
# =============================================================================

fact_orders = spark.read.parquet(FACT_ORDERS_PATH)

customer_summary = spark.read.parquet(
    CUSTOMER_SUMMARY_PATH
)

product_summary = spark.read.parquet(
    PRODUCT_SUMMARY_PATH
)

seller_summary = spark.read.parquet(
    SELLER_SUMMARY_PATH
)

payment_summary = spark.read.parquet(
    PAYMENT_SUMMARY_PATH
)

delivery_summary = spark.read.parquet(
    DELIVERY_SUMMARY_PATH
)

executive_summary = spark.read.parquet(
    EXECUTIVE_SUMMARY_PATH
)

print("All Gold tables loaded.")


# =============================================================================
# CALCULATE SOURCE-OF-TRUTH METRICS
# =============================================================================

print()
print("=" * 80)
print("CALCULATING SOURCE METRICS")
print("=" * 80)


# -----------------------------------------------------------------------------
# Fact orders
# -----------------------------------------------------------------------------

fact_metrics = fact_orders.agg(
    F.countDistinct("order_id").alias("total_orders"),
    F.sum("order_item_id").alias("total_items"),
    F.countDistinct("customer_unique_id").alias("total_customers"),
    F.countDistinct("product_id").alias("total_products"),
    F.countDistinct("seller_id").alias("total_sellers"),
    F.sum("item_revenue").alias("total_item_revenue"),
    F.sum("freight_value").alias("total_freight"),
    F.sum("total_item_value").alias("total_sales_value"),
)


# -----------------------------------------------------------------------------
# Customer summary
# -----------------------------------------------------------------------------

customer_metrics = customer_summary.agg(
    F.countDistinct("customer_unique_id").alias(
        "total_customers"
    )
)


# -----------------------------------------------------------------------------
# Product summary
# -----------------------------------------------------------------------------

product_metrics = product_summary.agg(
    F.countDistinct("product_id").alias(
        "total_products"
    )
)


# -----------------------------------------------------------------------------
# Seller summary
# -----------------------------------------------------------------------------

seller_metrics = seller_summary.agg(
    F.countDistinct("seller_id").alias(
        "total_sellers"
    )
)


# -----------------------------------------------------------------------------
# Payment summary
# -----------------------------------------------------------------------------

payment_metrics = payment_summary.agg(
    F.sum("total_payment_value").alias(
        "total_payment_value"
    )
)


# -----------------------------------------------------------------------------
# Delivery summary
# -----------------------------------------------------------------------------

delivery_metrics = delivery_summary.agg(
    F.sum("delivered_orders").alias(
        "delivered_orders"
    ),
    F.sum("shipped_orders").alias(
        "shipped_orders"
    ),
    F.sum("canceled_orders").alias(
        "canceled_orders"
    ),
    F.sum("late_deliveries").alias(
        "late_deliveries"
    ),
    F.sum("on_time_deliveries").alias(
        "on_time_deliveries"
    ),
)


# =============================================================================
# COLLECT METRICS
# =============================================================================

fact = fact_metrics.first()
customer = customer_metrics.first()
product = product_metrics.first()
seller = seller_metrics.first()
payment = payment_metrics.first()
delivery = delivery_metrics.first()
executive = executive_summary.first()


# =============================================================================
# RECONCILIATION FUNCTION
# =============================================================================

def check_metric(name, executive_value, source_value, tolerance=0.01):

    if executive_value is None and source_value is None:
        print(f"PASS  | {name}")
        return True

    if executive_value is None or source_value is None:
        print(
            f"FAIL  | {name} | "
            f"Executive={executive_value} | "
            f"Source={source_value}"
        )
        return False

    difference = abs(
        float(executive_value) - float(source_value)
    )

    if difference <= tolerance:
        print(
            f"PASS  | {name} | "
            f"Executive={executive_value} | "
            f"Source={source_value}"
        )
        return True

    print(
        f"FAIL  | {name} | "
        f"Executive={executive_value} | "
        f"Source={source_value} | "
        f"Difference={difference}"
    )

    return False


# =============================================================================
# RUN RECONCILIATION
# =============================================================================

print()
print("=" * 80)
print("RECONCILIATION RESULTS")
print("=" * 80)


results = []


# -----------------------------------------------------------------------------
# Fact order reconciliation
# -----------------------------------------------------------------------------

results.append(
    check_metric(
        "Total Orders",
        executive["total_orders"],
        fact["total_orders"],
    )
)

results.append(
    check_metric(
        "Total Items",
        executive["total_items"],
        fact["total_items"],
    )
)

results.append(
    check_metric(
        "Total Customers",
        executive["total_customers"],
        fact["total_customers"],
    )
)

results.append(
    check_metric(
        "Total Products",
        executive["total_products"],
        fact["total_products"],
    )
)

results.append(
    check_metric(
        "Total Sellers",
        executive["total_sellers"],
        fact["total_sellers"],
    )
)

results.append(
    check_metric(
        "Total Item Revenue",
        executive["total_item_revenue"],
        fact["total_item_revenue"],
    )
)

results.append(
    check_metric(
        "Total Freight",
        executive["total_freight"],
        fact["total_freight"],
    )
)

results.append(
    check_metric(
        "Total Sales Value",
        executive["total_sales_value"],
        fact["total_sales_value"],
    )
)


# -----------------------------------------------------------------------------
# Customer summary reconciliation
# -----------------------------------------------------------------------------

results.append(
    check_metric(
        "Customer Summary Customers",
        executive["total_customers"],
        customer["total_customers"],
    )
)


# -----------------------------------------------------------------------------
# Product summary reconciliation
# -----------------------------------------------------------------------------

results.append(
    check_metric(
        "Product Summary Products",
        executive["total_products"],
        product["total_products"],
    )
)


# -----------------------------------------------------------------------------
# Seller summary reconciliation
# -----------------------------------------------------------------------------

results.append(
    check_metric(
        "Seller Summary Sellers",
        executive["total_sellers"],
        seller["total_sellers"],
    )
)


# -----------------------------------------------------------------------------
# Payment reconciliation
# -----------------------------------------------------------------------------

results.append(
    check_metric(
        "Total Payment Value",
        executive["total_payment_value"],
        payment["total_payment_value"],
    )
)


# -----------------------------------------------------------------------------
# Delivery reconciliation
# -----------------------------------------------------------------------------

results.append(
    check_metric(
        "Delivered Orders",
        executive["delivered_orders"],
        delivery["delivered_orders"],
    )
)

results.append(
    check_metric(
        "Shipped Orders",
        executive["shipped_orders"],
        delivery["shipped_orders"],
    )
)

results.append(
    check_metric(
        "Canceled Orders",
        executive["canceled_orders"],
        delivery["canceled_orders"],
    )
)

results.append(
    check_metric(
        "Late Deliveries",
        executive["late_deliveries"],
        delivery["late_deliveries"],
    )
)

results.append(
    check_metric(
        "On-Time Deliveries",
        executive["on_time_deliveries"],
        delivery["on_time_deliveries"],
    )
)


# =============================================================================
# FINAL RESULT
# =============================================================================

print()
print("=" * 80)
print("RECONCILIATION SUMMARY")
print("=" * 80)

passed = sum(results)
failed = len(results) - passed

print(f"Checks passed: {passed}")
print(f"Checks failed: {failed}")


if failed > 0:
    raise ValueError(
        "Gold layer reconciliation failed. "
        "Review the failed metrics above."
    )


print()
print("=" * 80)
print("GOLD LAYER RECONCILIATION PASSED")
print("=" * 80)


# =============================================================================
# STOP SPARK
# =============================================================================

spark.stop()