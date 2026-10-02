from pathlib import Path

from pyspark.sql import functions as F

from src.ingestion.spark_session import create_spark_session


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_SELLERS_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "sellers"
)

GOLD_SELLERS_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "dim_sellers"
)


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    spark = create_spark_session()

    print("=" * 80)
    print("BUILDING GOLD DIM_SELLERS")
    print("=" * 80)

    # --------------------------------------------------------
    # 1. READ SILVER SELLERS
    # --------------------------------------------------------

    sellers_df = spark.read.parquet(
        str(SILVER_SELLERS_PATH)
    )

    print("Silver sellers loaded.")

    print(
        f"Silver seller rows: "
        f"{sellers_df.count()}"
    )

    # --------------------------------------------------------
    # 2. CLEAN SELLER ATTRIBUTES
    # --------------------------------------------------------

    dim_sellers = (
        sellers_df

        .withColumn(
            "seller_id",
            F.trim(F.col("seller_id"))
        )

        .withColumn(
            "seller_city",
            F.trim(
                F.lower(
                    F.col("seller_city")
                )
            )
        )

        .withColumn(
            "seller_state",
            F.trim(
                F.upper(
                    F.col("seller_state")
                )
            )
        )

        .select(
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state"
        )

        .dropDuplicates(
            ["seller_id"]
        )
    )

    print(
        "Seller attributes cleaned."
    )

    # --------------------------------------------------------
    # 3. WRITE GOLD DIMENSION
    # --------------------------------------------------------

    (
        dim_sellers.write
        .mode("overwrite")
        .parquet(
            str(GOLD_SELLERS_PATH)
        )
    )

    print(
        f"Gold dim_sellers written to:\n"
        f"{GOLD_SELLERS_PATH}"
    )

    # --------------------------------------------------------
    # 4. VALIDATION
    # --------------------------------------------------------

    print("\n" + "=" * 80)
    print("GOLD DIM_SELLERS VALIDATION")
    print("=" * 80)

    print(
        f"Rows: {dim_sellers.count()}"
    )

    print("\nSchema:")

    dim_sellers.printSchema()

    print("\nSample records:")

    dim_sellers.show(
        10,
        truncate=False
    )

    # --------------------------------------------------------
    # 5. CHECK SELLER KEY UNIQUENESS
    # --------------------------------------------------------

    duplicate_sellers = (
        dim_sellers
        .groupBy("seller_id")
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    print(
        f"\nDuplicate seller_id keys: "
        f"{duplicate_sellers}"
    )

    # --------------------------------------------------------
    # 6. CHECK NULL SELLER IDS
    # --------------------------------------------------------

    null_seller_ids = (
        dim_sellers
        .filter(
            F.col("seller_id").isNull()
        )
        .count()
    )

    print(
        f"Null seller_id keys: "
        f"{null_seller_ids}"
    )

    # --------------------------------------------------------
    # 7. COMPLETION
    # --------------------------------------------------------

    print("\n" + "=" * 80)
    print(
        "GOLD DIM_SELLERS COMPLETED SUCCESSFULLY"
    )
    print("=" * 80)

    spark.stop()