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

DELIVERY_SUMMARY_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "analytics"
    / "delivery_summary"
)


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    spark = create_spark_session()

    print("=" * 80)
    print("BUILDING GOLD ANALYTICS - DELIVERY SUMMARY")
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
    # 2. CREATE ONE ROW PER ORDER
    # --------------------------------------------------------
    #
    # fact_orders is at order-item grain.
    #
    # Delivery information belongs to the ORDER, not
    # individual order items.
    #
    # Therefore, we collapse the fact table to one row
    # per order before calculating delivery metrics.
    # --------------------------------------------------------

    orders = (
        fact_orders
        .groupBy(
            "order_id"
        )
        .agg(
            F.first(
                "customer_state",
                ignorenulls=True
            ).alias("customer_state"),

            F.first(
                "order_status",
                ignorenulls=True
            ).alias("order_status"),

            F.first(
                "order_purchase_timestamp",
                ignorenulls=True
            ).alias("order_purchase_timestamp"),

            F.first(
                "order_delivered_carrier_date",
                ignorenulls=True
            ).alias("order_delivered_carrier_date"),

            F.first(
                "order_delivered_customer_date",
                ignorenulls=True
            ).alias("order_delivered_customer_date"),

            F.first(
                "order_estimated_delivery_date",
                ignorenulls=True
            ).alias("order_estimated_delivery_date")
        )
    )

    print(
        f"Unique orders: {orders.count()}"
    )

    # --------------------------------------------------------
    # 3. CREATE ORDER DATE
    # --------------------------------------------------------

    orders = (
        orders
        .withColumn(
            "order_date",
            F.to_date(
                F.col("order_purchase_timestamp")
            )
        )
        .withColumn(
            "year",
            F.year(
                F.col("order_date")
            )
        )
        .withColumn(
            "month",
            F.month(
                F.col("order_date")
            )
        )
        .withColumn(
            "year_month",
            F.date_format(
                F.col("order_date"),
                "yyyy-MM"
            )
        )
    )

    # --------------------------------------------------------
    # 4. CALCULATE DELIVERY METRICS
    # --------------------------------------------------------

    orders = (
        orders

        # Actual delivery duration
        .withColumn(
            "delivery_days",
            F.when(
                F.col(
                    "order_delivered_customer_date"
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

        # Estimated delivery duration
        .withColumn(
            "estimated_delivery_days",
            F.when(
                F.col(
                    "order_estimated_delivery_date"
                ).isNotNull(),

                F.datediff(
                    F.to_date(
                        F.col(
                            "order_estimated_delivery_date"
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

        # Difference between actual and estimated delivery
        #
        # Positive = delivered late
        # Zero     = delivered on estimated date
        # Negative = delivered early
        #
        .withColumn(
            "delivery_delay_days",
            F.when(
                F.col(
                    "order_delivered_customer_date"
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

        # Late delivery flag
        .withColumn(
            "is_late_delivery",
            F.when(
                F.col(
                    "delivery_delay_days"
                ) > 0,
                1
            ).otherwise(0)
        )

        # On-time delivery flag
        .withColumn(
            "is_on_time_delivery",
            F.when(
                (
                    F.col(
                        "order_delivered_customer_date"
                    ).isNotNull()
                )
                &
                (
                    F.col(
                        "delivery_delay_days"
                    ) <= 0
                ),
                1
            ).otherwise(0)
        )
    )

    print(
        "Order-level delivery metrics calculated."
    )

    # --------------------------------------------------------
    # 5. BUILD DELIVERY SUMMARY
    # --------------------------------------------------------
    #
    # Grain:
    #
    # One row per:
    # customer_state + year_month + order_status
    #
    # This allows us to analyze:
    #
    # - delivery performance over time
    # - delivery performance by state
    # - order status distribution
    # - late delivery rate
    # --------------------------------------------------------

    delivery_summary = (
        orders
        .groupBy(
            "customer_state",
            "year",
            "month",
            "year_month",
            "order_status"
        )
        .agg(

            # Total orders
            F.countDistinct(
                "order_id"
            ).alias(
                "total_orders"
            ),

            # Orders that have actually been delivered
            F.countDistinct(
                F.when(
                    F.col(
                        "order_delivered_customer_date"
                    ).isNotNull(),
                    F.col("order_id")
                )
            ).alias(
                "delivered_orders"
            ),

            # Orders canceled
            F.countDistinct(
                F.when(
                    F.col(
                        "order_status"
                    ) == "canceled",
                    F.col("order_id")
                )
            ).alias(
                "canceled_orders"
            ),

            # Orders shipped
            F.countDistinct(
                F.when(
                    F.col(
                        "order_status"
                    ) == "shipped",
                    F.col("order_id")
                )
            ).alias(
                "shipped_orders"
            ),

            # Average actual delivery time
            F.avg(
                "delivery_days"
            ).alias(
                "average_delivery_days"
            ),

            # Average estimated delivery time
            F.avg(
                "estimated_delivery_days"
            ).alias(
                "average_estimated_delivery_days"
            ),

            # Average delivery delay
            F.avg(
                "delivery_delay_days"
            ).alias(
                "average_delivery_delay_days"
            ),

            # Number of late deliveries
            F.sum(
                "is_late_delivery"
            ).alias(
                "late_deliveries"
            ),

            # Number of on-time deliveries
            F.sum(
                "is_on_time_delivery"
            ).alias(
                "on_time_deliveries"
            )
        )
    )

    # --------------------------------------------------------
    # 6. CALCULATE ON-TIME DELIVERY RATE
    # --------------------------------------------------------

    delivery_summary = (
        delivery_summary
        .withColumn(
            "on_time_delivery_rate",
            F.when(
                F.col(
                    "delivered_orders"
                ) > 0,

                F.round(
                    (
                        F.col(
                            "on_time_deliveries"
                        )
                        /
                        F.col(
                            "delivered_orders"
                        )
                    )
                    * 100,
                    2
                )
            )
            .otherwise(0.0)
        )
    )

    # --------------------------------------------------------
    # 7. ROUND BUSINESS METRICS
    # --------------------------------------------------------

    delivery_summary = (
        delivery_summary

        .withColumn(
            "average_delivery_days",
            F.round(
                F.col(
                    "average_delivery_days"
                ),
                2
            )
        )

        .withColumn(
            "average_estimated_delivery_days",
            F.round(
                F.col(
                    "average_estimated_delivery_days"
                ),
                2
            )
        )

        .withColumn(
            "average_delivery_delay_days",
            F.round(
                F.col(
                    "average_delivery_delay_days"
                ),
                2
            )
        )
    )

    # --------------------------------------------------------
    # 8. ORDER COLUMNS
    # --------------------------------------------------------

    delivery_summary = delivery_summary.select(
        "customer_state",
        "year",
        "month",
        "year_month",
        "order_status",
        "total_orders",
        "delivered_orders",
        "canceled_orders",
        "shipped_orders",
        "average_delivery_days",
        "average_estimated_delivery_days",
        "average_delivery_delay_days",
        "late_deliveries",
        "on_time_deliveries",
        "on_time_delivery_rate"
    )

    # --------------------------------------------------------
    # 9. VALIDATION
    # --------------------------------------------------------

    print("\n" + "=" * 80)
    print("DELIVERY SUMMARY VALIDATION")
    print("=" * 80)

    print(
        f"Rows: {delivery_summary.count()}"
    )

    print("\nSchema:")

    delivery_summary.printSchema()

    print("\nSample records:")

    delivery_summary.show(
        10,
        truncate=False
    )

    # --------------------------------------------------------
    # 10. KEY VALIDATIONS
    # --------------------------------------------------------

    duplicate_keys = (
        delivery_summary
        .groupBy(
            "customer_state",
            "year_month",
            "order_status"
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    null_state_keys = (
        delivery_summary
        .filter(
            F.col(
                "customer_state"
            ).isNull()
        )
        .count()
    )

    print(
        f"Duplicate delivery summary keys: "
        f"{duplicate_keys}"
    )

    print(
        f"Null customer_state keys: "
        f"{null_state_keys}"
    )

    # --------------------------------------------------------
    # 11. WRITE ANALYTICS TABLE
    # --------------------------------------------------------

    (
        delivery_summary.write
        .mode("overwrite")
        .parquet(
            str(DELIVERY_SUMMARY_PATH)
        )
    )

    print(
        "\nDelivery summary written to:"
    )

    print(
        DELIVERY_SUMMARY_PATH
    )

    # --------------------------------------------------------
    # 12. FINAL MESSAGE
    # --------------------------------------------------------

    print("\n" + "=" * 80)
    print(
        "GOLD DELIVERY SUMMARY COMPLETED SUCCESSFULLY"
    )
    print("=" * 80)

    spark.stop()