from src.ingestion.spark_session import create_spark_session


spark = create_spark_session()

print("Spark session created successfully!")
print("Spark version:", spark.version)

spark.stop()
print("Spark session stopped successfully!")