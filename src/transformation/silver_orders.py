from pathlib import Path

from pyspark.sql import functions as F

from src.ingestion.spark_session import create_spark_session


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_DATA_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DATA_DIR = PROJECT_ROOT / "data" / "silver"


# ============================================================
# TRANSFORMATION
# ============================================================

def transform_orders(spark):
    """
    Read Bronze orders and transform them into the Silver layer.
    """

    bronze_path = BRONZE_DATA_DIR / "orders"

    orders_df = spark.read.parquet(
        str(bronze_path)
    )

    print("Bronze orders loaded.")
    print(f"Bronze rows: {orders_df.count()}")

    # --------------------------------------------------------
    # Clean and standardize order columns
    # --------------------------------------------------------

    orders_silver_df = (
        orders_df

        .withColumn(
            "order_id",
            F.trim(F.col("order_id"))
        )

        .withColumn(
            "customer_id",
            F.trim(F.col("customer_id"))
        )

        .withColumn(
            "order_status",
            F.lower(
                F.trim(
                    F.col("order_status")
                )
            )
        )

        # ----------------------------------------------------
        # Convert order timestamps
        # ----------------------------------------------------

        .withColumn(
            "order_purchase_timestamp",
            F.expr(
                "try_cast(trim(order_purchase_timestamp) AS TIMESTAMP)"
            )
        )

        .withColumn(
            "order_approved_at",
            F.expr(
                "try_cast(trim(order_approved_at) AS TIMESTAMP)"
            )
        )

        .withColumn(
            "order_delivered_carrier_date",
            F.expr(
                "try_cast(trim(order_delivered_carrier_date) AS TIMESTAMP)"
            )
        )

        .withColumn(
            "order_delivered_customer_date",
            F.expr(
                "try_cast(trim(order_delivered_customer_date) AS TIMESTAMP)"
            )
        )

        .withColumn(
            "order_estimated_delivery_date",
            F.expr(
                "try_cast(trim(order_estimated_delivery_date) AS TIMESTAMP)"
            )
        )

        # ----------------------------------------------------
        # Remove invalid records
        # ----------------------------------------------------

        .filter(
            F.col("order_id").isNotNull()
        )

        .filter(
            F.col("customer_id").isNotNull()
        )

        # ----------------------------------------------------
        # Remove duplicate orders
        # ----------------------------------------------------

        .dropDuplicates(
            ["order_id"]
        )
    )

    return orders_silver_df


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    spark = create_spark_session()

    orders_silver_df = transform_orders(spark)

    print()
    print("Silver orders transformation completed!")
    print(
        f"Number of rows: "
        f"{orders_silver_df.count()}"
    )

    print()
    print("Silver schema:")

    orders_silver_df.printSchema()

    print()
    print("Sample Silver records:")

    orders_silver_df.show(
        5,
        truncate=False
    )

    # --------------------------------------------------------
    # Write Silver orders
    # --------------------------------------------------------

    silver_path = SILVER_DATA_DIR / "orders"

    (
        orders_silver_df.write
        .mode("overwrite")
        .parquet(
            str(silver_path)
        )
    )

    print()
    print(
        f"Silver orders written to: "
        f"{silver_path}"
    )

    spark.stop()