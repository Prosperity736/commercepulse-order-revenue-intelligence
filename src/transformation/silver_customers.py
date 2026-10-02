from pathlib import Path

from pyspark.sql import functions as F

from src.ingestion.spark_session import create_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_DATA_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DATA_DIR = PROJECT_ROOT / "data" / "silver"


def transform_customers(spark):
    """
    Read Bronze customers and transform them into the Silver layer.
    """

    bronze_path = BRONZE_DATA_DIR / "customers"

    customers_df = spark.read.parquet(
        str(bronze_path)
    )

    customers_silver_df = (
        customers_df
        .withColumn(
            "customer_city",
            F.trim(F.col("customer_city"))
        )
        .withColumn(
            "customer_state",
            F.trim(F.col("customer_state"))
        )
        .dropDuplicates(["customer_id"])
    )

    return customers_silver_df


if __name__ == "__main__":

    spark = create_spark_session()

    customers_silver_df = transform_customers(spark)

    print("Silver customers transformation completed!")
    print(
        f"Number of rows: "
        f"{customers_silver_df.count()}"
    )

    customers_silver_df.printSchema()

    customers_silver_df.show(
        5,
        truncate=False
    )

    silver_path = SILVER_DATA_DIR / "customers"

    customers_silver_df.write \
        .mode("overwrite") \
        .parquet(str(silver_path))

    print(
        f"Silver customers written to: "
        f"{silver_path}"
    )

    spark.stop()