from pathlib import Path

from pyspark.sql import functions as F

from src.ingestion.spark_session import create_spark_session


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_PRODUCTS_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "products"
)

SILVER_CATEGORY_TRANSLATION_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "product_category_translation"
)

GOLD_PRODUCTS_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "dim_products"
)


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    spark = create_spark_session()

    print("=" * 80)
    print("BUILDING GOLD DIM_PRODUCTS")
    print("=" * 80)

    # --------------------------------------------------------
    # 1. READ SILVER PRODUCTS
    # --------------------------------------------------------

    products_df = spark.read.parquet(
        str(SILVER_PRODUCTS_PATH)
    )

    print("Silver products loaded.")
    print(
        f"Silver product rows: "
        f"{products_df.count()}"
    )

    # --------------------------------------------------------
    # 2. READ CATEGORY TRANSLATION
    # --------------------------------------------------------

    translation_df = spark.read.parquet(
        str(SILVER_CATEGORY_TRANSLATION_PATH)
    )

    print("Silver category translation loaded.")
    print(
        f"Translation rows: "
        f"{translation_df.count()}"
    )

    # --------------------------------------------------------
    # 3. PREPARE PRODUCT DATA
    # --------------------------------------------------------

    products_df = (
        products_df

        .withColumn(
            "product_id",
            F.trim(F.col("product_id"))
        )

        .withColumn(
            "product_category_name",
            F.trim(
                F.lower(
                    F.col("product_category_name")
                )
            )
        )
    )

    # --------------------------------------------------------
    # 4. PREPARE CATEGORY TRANSLATION
    # --------------------------------------------------------

    translation_df = (
        translation_df

        .withColumn(
            "product_category_name",
            F.trim(
                F.lower(
                    F.col("product_category_name")
                )
            )
        )

        .withColumn(
            "product_category_name_english",
            F.trim(
                F.lower(
                    F.col(
                        "product_category_name_english"
                    )
                )
            )
        )

        .dropDuplicates(
            ["product_category_name"]
        )
    )

    # --------------------------------------------------------
    # 5. JOIN PRODUCTS WITH CATEGORY TRANSLATION
    # --------------------------------------------------------

    dim_products = (
        products_df
        .join(
            translation_df,
            on="product_category_name",
            how="left"
        )
        .select(
            "product_id",
            "product_category_name",
            "product_category_name_english",
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm"
        )
        .dropDuplicates(
            ["product_id"]
        )
    )

    print(
        "Products joined with category translation."
    )

    # --------------------------------------------------------
    # 6. WRITE GOLD DIMENSION
    # --------------------------------------------------------

    (
        dim_products.write
        .mode("overwrite")
        .parquet(
            str(GOLD_PRODUCTS_PATH)
        )
    )

    print(
        f"Gold dim_products written to:\n"
        f"{GOLD_PRODUCTS_PATH}"
    )

    # --------------------------------------------------------
    # 7. VALIDATION
    # --------------------------------------------------------

    print("\n" + "=" * 80)
    print("GOLD DIM_PRODUCTS VALIDATION")
    print("=" * 80)

    print(
        f"Rows: {dim_products.count()}"
    )

    print("\nSchema:")

    dim_products.printSchema()

    print("\nSample records:")

    dim_products.show(
        10,
        truncate=False
    )

    # --------------------------------------------------------
    # 8. CHECK PRODUCT KEY UNIQUENESS
    # --------------------------------------------------------

    duplicate_products = (
        dim_products
        .groupBy("product_id")
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    print(
        f"\nDuplicate product_id keys: "
        f"{duplicate_products}"
    )

    # --------------------------------------------------------
    # 9. CHECK CATEGORY TRANSLATION COVERAGE
    # --------------------------------------------------------

    total_products = dim_products.count()

    translated_products = (
        dim_products
        .filter(
            F.col(
                "product_category_name_english"
            ).isNotNull()
        )
        .count()
    )

    untranslated_products = (
        total_products - translated_products
    )

    print(
        f"Products with English category: "
        f"{translated_products}"
    )

    print(
        f"Products without English category: "
        f"{untranslated_products}"
    )

    # --------------------------------------------------------
    # 10. COMPLETION
    # --------------------------------------------------------

    print("\n" + "=" * 80)
    print(
        "GOLD DIM_PRODUCTS COMPLETED SUCCESSFULLY"
    )
    print("=" * 80)

    spark.stop()