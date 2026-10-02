from pathlib import Path

from pyspark.sql import functions as F

from src.ingestion.spark_session import create_spark_session


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_CUSTOMERS_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "customers"
)

GOLD_CUSTOMERS_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "dim_customers"
)


# ============================================================
# BUILD GOLD CUSTOMER DIMENSION
# ============================================================

if __name__ == "__main__":

    spark = create_spark_session()

    print("=" * 80)
    print("BUILDING GOLD DIM_CUSTOMERS")
    print("=" * 80)

    # --------------------------------------------------------
    # 1. READ SILVER CUSTOMERS
    # --------------------------------------------------------

    customers_df = spark.read.parquet(
        str(SILVER_CUSTOMERS_PATH)
    )

    print("Silver customers loaded.")
    print(f"Silver rows: {customers_df.count()}")

    # --------------------------------------------------------
    # 2. CLEAN CUSTOMER ATTRIBUTES
    # --------------------------------------------------------

    dim_customers = (
        customers_df

        .withColumn(
            "customer_id",
            F.trim(F.col("customer_id"))
        )

        .withColumn(
            "customer_unique_id",
            F.trim(F.col("customer_unique_id"))
        )

        .withColumn(
            "customer_city",
            F.trim(F.lower(F.col("customer_city")))
        )

        .withColumn(
            "customer_state",
            F.trim(F.upper(F.col("customer_state")))
        )

        .select(
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state"
        )

        .dropDuplicates(
            ["customer_id"]
        )
    )

    # --------------------------------------------------------
    # 3. WRITE GOLD DIMENSION
    # --------------------------------------------------------

    (
        dim_customers.write
        .mode("overwrite")
        .parquet(
            str(GOLD_CUSTOMERS_PATH)
        )
    )

    print(
        f"Gold dim_customers written to:\n"
        f"{GOLD_CUSTOMERS_PATH}"
    )

    # --------------------------------------------------------
    # 4. VALIDATION
    # --------------------------------------------------------

    print("\n" + "=" * 80)
    print("GOLD DIM_CUSTOMERS VALIDATION")
    print("=" * 80)

    print(
        f"Rows: {dim_customers.count()}"
    )

    print("\nSchema:")

    dim_customers.printSchema()

    print("\nSample records:")

    dim_customers.show(
        5,
        truncate=False
    )

    # Check customer_id uniqueness
    duplicate_customers = (
        dim_customers
        .groupBy("customer_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print(
        f"\nDuplicate customer_id keys: "
        f"{duplicate_customers}"
    )

    print("\n" + "=" * 80)
    print("GOLD DIM_CUSTOMERS COMPLETED SUCCESSFULLY")
    print("=" * 80)

    spark.stop()