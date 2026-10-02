from pyspark.sql import SparkSession


def create_spark_session():
    """
    Create a local Spark session configured for Windows.

    Hadoop native libraries such as winutils.exe are not required
    for this local project. RawLocalFileSystem is used to avoid
    Windows NativeIO access issues.
    """

    spark = (
        SparkSession.builder
        .appName("OlistSupplyChain")
        .master("local[*]")

        # Windows / Hadoop configuration
        .config(
            "spark.hadoop.fs.file.impl",
            "org.apache.hadoop.fs.RawLocalFileSystem"
        )
        .config(
            "spark.hadoop.fs.file.impl.disable.cache",
            "true"
        )
        .config(
            "spark.hadoop.io.native.lib.available",
            "false"
        )

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark