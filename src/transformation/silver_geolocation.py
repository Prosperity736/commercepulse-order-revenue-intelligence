from pathlib import Path

from pyspark.sql import functions as F

from src.ingestion.spark_session import create_spark_session


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_GEOLOCATION_PATH = (
    PROJECT_ROOT
    / "data"
    / "bronze"
    / "geolocation"
)

SILVER_GEOLOCATION_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "geolocation"
)


# ============================================================
# MAIN TRANSFORMATION
# ============================================================

if __name__ == "__main__":

    spark = create_spark_session()

    # --------------------------------------------------------
    # 1. READ BRONZE GEOLOCATION
    # --------------------------------------------------------

    geolocation_df = spark.read.parquet(
        str(BRONZE_GEOLOCATION_PATH)
    )

    print("Bronze geolocation loaded.")

    print(
        f"Bronze rows: {geolocation_df.count()}"
    )

    geolocation_df.printSchema()


    # --------------------------------------------------------
    # 2. CLEAN AND STANDARDIZE GEOLOCATION DATA
    # --------------------------------------------------------

    silver_geolocation_df = (
        geolocation_df

        # ZIP CODE
        .withColumn(
            "geolocation_zip_code_prefix",
            F.trim(
                F.col("geolocation_zip_code_prefix")
            )
        )

        # LATITUDE
        .withColumn(
            "geolocation_lat",
            F.expr(
                "try_cast(trim(geolocation_lat) AS DOUBLE)"
            )
        )

        # LONGITUDE
        .withColumn(
            "geolocation_lng",
            F.expr(
                "try_cast(trim(geolocation_lng) AS DOUBLE)"
            )
        )

        # CITY
        .withColumn(
            "geolocation_city",
            F.trim(
                F.lower(
                    F.col("geolocation_city")
                )
            )
        )

        # STATE
        .withColumn(
            "geolocation_state",
            F.trim(
                F.upper(
                    F.col("geolocation_state")
                )
            )
        )
    )


    # --------------------------------------------------------
    # 3. REMOVE INVALID GEOLOCATION RECORDS
    # --------------------------------------------------------

    silver_geolocation_df = (
        silver_geolocation_df

        .filter(
            F.col("geolocation_zip_code_prefix").isNotNull()
        )

        .filter(
            F.col("geolocation_lat").isNotNull()
        )

        .filter(
            F.col("geolocation_lng").isNotNull()
        )

        .filter(
            F.col("geolocation_city").isNotNull()
        )

        .filter(
            F.col("geolocation_state").isNotNull()
        )
    )


    # --------------------------------------------------------
    # 4. VALIDATE LATITUDE AND LONGITUDE
    # --------------------------------------------------------

    silver_geolocation_df = (
        silver_geolocation_df

        .filter(
            F.col("geolocation_lat").between(
                -90,
                90
            )
        )

        .filter(
            F.col("geolocation_lng").between(
                -180,
                180
            )
        )
    )


    # --------------------------------------------------------
    # 5. REMOVE DUPLICATE GEOLOCATION RECORDS
    # --------------------------------------------------------

    silver_geolocation_df = (
        silver_geolocation_df
        .dropDuplicates()
    )


    # --------------------------------------------------------
    # 6. SHOW TRANSFORMATION RESULTS
    # --------------------------------------------------------

    print(
        "Silver geolocation transformation completed!"
    )

    print(
        f"Number of rows: "
        f"{silver_geolocation_df.count()}"
    )

    print("\nSilver schema:")

    silver_geolocation_df.printSchema()

    print("\nSample Silver records:")

    silver_geolocation_df.show(
        5,
        truncate=False
    )


    # --------------------------------------------------------
    # 7. WRITE SILVER GEOLOCATION
    # --------------------------------------------------------

    (
        silver_geolocation_df.write
        .mode("overwrite")
        .parquet(
            str(SILVER_GEOLOCATION_PATH)
        )
    )

    print(
        f"\nSilver geolocation written to: "
        f"{SILVER_GEOLOCATION_PATH}"
    )


    # --------------------------------------------------------
    # 8. STOP SPARK
    # --------------------------------------------------------

    spark.stop()