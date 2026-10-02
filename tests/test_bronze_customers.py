from src.ingestion.spark_session import create_spark_session


def main():

    spark = create_spark_session()

    bronze_path = "data/bronze/customers"

    customers_df = spark.read.parquet(bronze_path)

    print("Bronze customers data loaded successfully!")
    print(f"Number of rows: {customers_df.count()}")

    customers_df.printSchema()

    spark.stop()


if __name__ == "__main__":
    main()