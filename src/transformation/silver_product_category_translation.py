from pathlib import Path

from pyspark.sql import functions as F

from src.ingestion.spark_session import create_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_PRODUCT_CATEGORY_TRANSLATION_PATH = (
    PROJECT_ROOT
    / "data"
    / "bronze"
    / "product_category_translation"
)

SILVER_PRODUCT_CATEGORY_TRANSLATION_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "product_category_translation"
)


if __name__ == "__main__":

    spark = create_spark_session()

    # Read Bronze product category translation
    category_translation_df = spark.read.parquet(
        str(BRONZE_PRODUCT_CATEGORY_TRANSLATION_PATH)
    )

    print("Bronze product category translation loaded.")
    print(
        f"Bronze rows: "
        f"{category_translation_df.count()}"
    )

    print("Bronze schema:")
    category_translation_df.printSchema()

    # Transform product category translation
    silver_category_translation_df = (
        category_translation_df
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
                    F.col("product_category_name_english")
                )
            )
        )
        .dropDuplicates(
            ["product_category_name"]
        )
    )

    print(
        "Silver product category translation "
        "transformation completed!"
    )

    print(
        f"Number of rows: "
        f"{silver_category_translation_df.count()}"
    )

    print("Silver schema:")
    silver_category_translation_df.printSchema()

    print("Sample Silver records:")
    silver_category_translation_df.show(
        10,
        truncate=False
    )

    # Write Silver product category translation
    silver_category_translation_df.write \
        .mode("overwrite") \
        .parquet(
            str(
                SILVER_PRODUCT_CATEGORY_TRANSLATION_PATH
            )
        )

    print(
        "Silver product category translation "
        f"written to: "
        f"{SILVER_PRODUCT_CATEGORY_TRANSLATION_PATH}"
    )

    spark.stop()