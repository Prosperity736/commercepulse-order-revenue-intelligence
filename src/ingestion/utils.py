def load_csv(spark, file_path):
    """
    Load a CSV file into a Spark DataFrame.

    Bronze layer:
    - Preserve all values as strings
    - Support quoted fields
    - Support multiline text fields
    - Handle UTF-8 encoded data
    """

    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .option("encoding", "UTF-8")
        .csv(str(file_path))
    )


def write_parquet(df, output_path):
    """
    Write a Spark DataFrame to Parquet format.
    """

    (
        df.write
        .mode("overwrite")
        .parquet(str(output_path))
    )


def ingest_dataset(
    spark,
    raw_data_dir,
    bronze_data_dir,
    csv_filename,
    bronze_name
):
    """
    Load a CSV dataset and write it to the Bronze layer.
    """

    raw_path = raw_data_dir / csv_filename
    bronze_path = bronze_data_dir / bronze_name

    df = load_csv(
        spark,
        raw_path
    )

    write_parquet(
        df,
        bronze_path
    )

    return df