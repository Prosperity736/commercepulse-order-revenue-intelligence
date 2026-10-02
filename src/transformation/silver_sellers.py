from pathlib import Path

from pyspark.sql import functions as F

from src.ingestion.spark_session import create_spark_session


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_SELLERS_PATH = (
    PROJECT_ROOT
    / "data"
    / "bronze"
    / "sellers"
)

SILVER_SELLERS_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "sellers"
)


# ============================================================
# MAIN TRANSFORMATION
# ============================================================

if __name__ == "__main__":

    spark = create_spark_session()

    # --------------------------------------------------------
    # 1. READ BRONZE SELLERS
    # --------------------------------------------------------

    sellers_df = spark.read.parquet(
        str(BRONZE_SELLERS_PATH)
    )

    print("Bronze sellers loaded.")
    print(
        f"Bronze rows: {sellers_df.count()}"
    )

    print("\nBronze schema:")
    sellers_df.printSchema()


    # --------------------------------------------------------
    # 2. CLEAN SELLER CITY
    # --------------------------------------------------------

    silver_sellers_df = (
        sellers_df

        .withColumn(
            "seller_city",
            F.trim(
                F.lower(
                    F.col("seller_city")
                )
            )
        )

        # ----------------------------------------------------
        # 3. CLEAN SELLER STATE
        # ----------------------------------------------------

        .withColumn(
            "seller_state",
            F.trim(
                F.upper(
                    F.col("seller_state")
                )
            )
        )

        # ----------------------------------------------------
        # 4. CLEAN SELLER ZIP CODE
        # ----------------------------------------------------

        .withColumn(
            "seller_zip_code_prefix",
            F.expr(
                "try_cast(trim(seller_zip_code_prefix) AS INT)"
            )
        )

        # ----------------------------------------------------
        # 5. REMOVE INVALID SELLERS
        # ----------------------------------------------------

        .filter(
            F.col("seller_id").isNotNull()
        )

        # ----------------------------------------------------
        # 6. REMOVE DUPLICATE SELLERS
        # ----------------------------------------------------

        .dropDuplicates(
            ["seller_id"]
        )
    )


    # --------------------------------------------------------
    # 7. SHOW TRANSFORMATION RESULTS
    # --------------------------------------------------------

    print(
        "\nSilver sellers transformation completed!"
    )

    print(
        f"Number of rows: "
        f"{silver_sellers_df.count()}"
    )

    print("\nSilver schema:")
    silver_sellers_df.printSchema()

    print("\nSample Silver records:")

    silver_sellers_df.show(
        5,
        truncate=False
    )


    # --------------------------------------------------------
    # 8. WRITE SILVER DATA
    # --------------------------------------------------------

    (
        silver_sellers_df.write
        .mode("overwrite")
        .parquet(
            str(SILVER_SELLERS_PATH)
        )
    )

    print(
        f"\nSilver sellers written to: "
        f"{SILVER_SELLERS_PATH}"
    )


    # --------------------------------------------------------
    # 9. STOP SPARK
    # --------------------------------------------------------

    spark.stop()