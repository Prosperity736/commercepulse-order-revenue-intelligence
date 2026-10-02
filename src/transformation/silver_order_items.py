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

def transform_order_items(spark):
    """
    Read Bronze order items and transform them
    into the Silver layer.
    """

    bronze_path = BRONZE_DATA_DIR / "order_items"

    order_items_df = spark.read.parquet(
        str(bronze_path)
    )

    print("Bronze order items loaded.")
    print(
        f"Bronze rows: "
        f"{order_items_df.count()}"
    )

    # --------------------------------------------------------
    # Clean and standardize order item columns
    # --------------------------------------------------------

    order_items_silver_df = (
        order_items_df

        # ----------------------------------------------------
        # String columns
        # ----------------------------------------------------

        .withColumn(
            "order_id",
            F.trim(F.col("order_id"))
        )

        .withColumn(
            "product_id",
            F.trim(F.col("product_id"))
        )

        .withColumn(
            "seller_id",
            F.trim(F.col("seller_id"))
        )

        # ----------------------------------------------------
        # Order item ID
        # ----------------------------------------------------

        .withColumn(
            "order_item_id",
            F.expr(
                "try_cast(trim(order_item_id) AS INT)"
            )
        )

        # ----------------------------------------------------
        # Numeric columns
        # ----------------------------------------------------

        .withColumn(
            "price",
            F.expr(
                "try_cast(trim(price) AS DOUBLE)"
            )
        )

        .withColumn(
            "freight_value",
            F.expr(
                "try_cast(trim(freight_value) AS DOUBLE)"
            )
        )

        # ----------------------------------------------------
        # Shipping limit date
        # ----------------------------------------------------

        .withColumn(
            "shipping_limit_date",
            F.expr(
                "try_cast(trim(shipping_limit_date) AS TIMESTAMP)"
            )
        )

        # ----------------------------------------------------
        # Remove invalid records
        # ----------------------------------------------------

        .filter(
            F.col("order_id").isNotNull()
        )

        .filter(
            F.col("order_item_id").isNotNull()
        )

        .filter(
            F.col("product_id").isNotNull()
        )

        .filter(
            F.col("seller_id").isNotNull()
        )

        # ----------------------------------------------------
        # Remove duplicate order items
        # ----------------------------------------------------

        .dropDuplicates(
            ["order_id", "order_item_id"]
        )
    )

    return order_items_silver_df


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    spark = create_spark_session()

    order_items_silver_df = transform_order_items(
        spark
    )

    print()
    print(
        "Silver order_items transformation completed!"
    )

    print(
        f"Number of rows: "
        f"{order_items_silver_df.count()}"
    )

    print()
    print("Silver schema:")

    order_items_silver_df.printSchema()

    print()
    print("Sample Silver records:")

    order_items_silver_df.show(
        5,
        truncate=False
    )

    # --------------------------------------------------------
    # Write Silver order items
    # --------------------------------------------------------

    silver_path = (
        SILVER_DATA_DIR / "order_items"
    )

    (
        order_items_silver_df.write
        .mode("overwrite")
        .parquet(
            str(silver_path)
        )
    )

    print()
    print(
        f"Silver order_items written to: "
        f"{silver_path}"
    )

    spark.stop()