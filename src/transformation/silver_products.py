from pathlib import Path

from pyspark.sql import functions as F

from src.ingestion.spark_session import create_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_PRODUCTS_PATH = (
    PROJECT_ROOT
    / "data"
    / "bronze"
    / "products"
)

SILVER_PRODUCTS_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "products"
)


if __name__ == "__main__":

    spark = create_spark_session()

    # --------------------------------------------------------
    # 1. READ BRONZE PRODUCTS
    # --------------------------------------------------------

    products_df = spark.read.parquet(
        str(BRONZE_PRODUCTS_PATH)
    )

    print("Bronze products loaded.")

    print(
        f"Bronze rows: {products_df.count()}"
    )

    print("\nBronze schema:")
    products_df.printSchema()


    # --------------------------------------------------------
    # 2. CLEAN AND CAST PRODUCT COLUMNS
    # --------------------------------------------------------

    silver_products_df = (
        products_df

        # Product ID
        .withColumn(
            "product_id",
            F.trim(F.col("product_id"))
        )

        # Product category
        .withColumn(
            "product_category_name",
            F.when(
                F.trim(
                    F.col("product_category_name")
                ) == "",
                None
            ).otherwise(
                F.trim(
                    F.lower(
                        F.col("product_category_name")
                    )
                )
            )
        )

        # Product name length
        .withColumn(
            "product_name_lenght",
            F.expr(
                "try_cast(trim(product_name_lenght) AS INT)"
            )
        )

        # Product description length
        .withColumn(
            "product_description_lenght",
            F.expr(
                "try_cast(trim(product_description_lenght) AS INT)"
            )
        )

        # Product photos quantity
        .withColumn(
            "product_photos_qty",
            F.expr(
                "try_cast(trim(product_photos_qty) AS INT)"
            )
        )

        # Product weight
        .withColumn(
            "product_weight_g",
            F.expr(
                "try_cast(trim(product_weight_g) AS DOUBLE)"
            )
        )

        # Product length
        .withColumn(
            "product_length_cm",
            F.expr(
                "try_cast(trim(product_length_cm) AS DOUBLE)"
            )
        )

        # Product height
        .withColumn(
            "product_height_cm",
            F.expr(
                "try_cast(trim(product_height_cm) AS DOUBLE)"
            )
        )

        # Product width
        .withColumn(
            "product_width_cm",
            F.expr(
                "try_cast(trim(product_width_cm) AS DOUBLE)"
            )
        )
    )


    # --------------------------------------------------------
    # 3. HANDLE NULL NUMERIC VALUES
    # --------------------------------------------------------

    silver_products_df = (
        silver_products_df

        .withColumn(
            "product_name_lenght",
            F.coalesce(
                F.col("product_name_lenght"),
                F.lit(0)
            )
        )

        .withColumn(
            "product_description_lenght",
            F.coalesce(
                F.col("product_description_lenght"),
                F.lit(0)
            )
        )

        .withColumn(
            "product_photos_qty",
            F.coalesce(
                F.col("product_photos_qty"),
                F.lit(0)
            )
        )

        .withColumn(
            "product_weight_g",
            F.coalesce(
                F.col("product_weight_g"),
                F.lit(0.0)
            )
        )

        .withColumn(
            "product_length_cm",
            F.coalesce(
                F.col("product_length_cm"),
                F.lit(0.0)
            )
        )

        .withColumn(
            "product_height_cm",
            F.coalesce(
                F.col("product_height_cm"),
                F.lit(0.0)
            )
        )

        .withColumn(
            "product_width_cm",
            F.coalesce(
                F.col("product_width_cm"),
                F.lit(0.0)
            )
        )
    )


    # --------------------------------------------------------
    # 4. VALIDATE PRODUCT MEASUREMENTS
    # --------------------------------------------------------

    silver_products_df = (
        silver_products_df

        .filter(
            F.col("product_name_lenght") >= 0
        )

        .filter(
            F.col("product_description_lenght") >= 0
        )

        .filter(
            F.col("product_photos_qty") >= 0
        )

        .filter(
            F.col("product_weight_g") >= 0
        )

        .filter(
            F.col("product_length_cm") >= 0
        )

        .filter(
            F.col("product_height_cm") >= 0
        )

        .filter(
            F.col("product_width_cm") >= 0
        )
    )


    # --------------------------------------------------------
    # 5. REMOVE INVALID PRODUCT IDs
    # --------------------------------------------------------

    silver_products_df = (
        silver_products_df
        .filter(
            F.col("product_id").isNotNull()
        )
    )


    # --------------------------------------------------------
    # 6. REMOVE DUPLICATE PRODUCTS
    # --------------------------------------------------------

    silver_products_df = (
        silver_products_df
        .dropDuplicates(
            ["product_id"]
        )
    )


    # --------------------------------------------------------
    # 7. SELECT FINAL SILVER COLUMNS
    # --------------------------------------------------------

    silver_products_df = silver_products_df.select(
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    )


    # --------------------------------------------------------
    # 8. DISPLAY TRANSFORMATION RESULTS
    # --------------------------------------------------------

    print(
        "\nSilver products transformation completed!"
    )

    print(
        f"Number of rows: "
        f"{silver_products_df.count()}"
    )

    print("\nSilver schema:")
    silver_products_df.printSchema()

    print("\nSample Silver records:")

    silver_products_df.show(
        5,
        truncate=False
    )


    # --------------------------------------------------------
    # 9. WRITE SILVER PRODUCTS
    # --------------------------------------------------------

    (
        silver_products_df.write
        .mode("overwrite")
        .parquet(
            str(SILVER_PRODUCTS_PATH)
        )
    )


    print(
        f"\nSilver products written to: "
        f"{SILVER_PRODUCTS_PATH}"
    )


    # --------------------------------------------------------
    # 10. STOP SPARK
    # --------------------------------------------------------

    spark.stop()