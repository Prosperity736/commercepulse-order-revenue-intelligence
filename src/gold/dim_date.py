from pathlib import Path

from pyspark.sql import functions as F

from src.ingestion.spark_session import create_spark_session


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

GOLD_FACT_ORDERS_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "fact_orders"
)

GOLD_DATE_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "dim_date"
)


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    spark = create_spark_session()

    print("=" * 80)
    print("BUILDING GOLD DIM_DATE")
    print("=" * 80)

    # --------------------------------------------------------
    # 1. READ GOLD FACT ORDERS
    # --------------------------------------------------------

    fact_orders = spark.read.parquet(
        str(GOLD_FACT_ORDERS_PATH)
    )

    print("Gold fact_orders loaded.")

    # --------------------------------------------------------
    # 2. GET ORDER DATE RANGE
    # --------------------------------------------------------

    date_range = (
        fact_orders
        .select(
            F.to_date(
                F.col("order_purchase_timestamp")
            ).alias("order_date")
        )
        .agg(
            F.min("order_date").alias("min_date"),
            F.max("order_date").alias("max_date")
        )
        .collect()[0]
    )

    min_date = date_range["min_date"]
    max_date = date_range["max_date"]

    print(f"Minimum order date: {min_date}")
    print(f"Maximum order date: {max_date}")

    # --------------------------------------------------------
    # 3. GENERATE DATE RANGE
    # --------------------------------------------------------

    date_df = (
        spark.range(1)
        .select(
            F.explode(
                F.sequence(
                    F.lit(min_date),
                    F.lit(max_date),
                    F.expr("INTERVAL 1 DAY")
                )
            ).alias("date")
        )
    )

    # --------------------------------------------------------
    # 4. BUILD DATE ATTRIBUTES
    # --------------------------------------------------------

    dim_date = (
        date_df

        .withColumn(
            "date_key",
            F.date_format(
                F.col("date"),
                "yyyyMMdd"
            ).cast("int")
        )

        .withColumn(
            "year",
            F.year("date")
        )

        .withColumn(
            "quarter",
            F.quarter("date")
        )

        .withColumn(
            "quarter_name",
            F.concat(
                F.lit("Q"),
                F.quarter("date")
            )
        )

        .withColumn(
            "month",
            F.month("date")
        )

        .withColumn(
            "month_name",
            F.date_format(
                F.col("date"),
                "MMMM"
            )
        )

        .withColumn(
            "month_short_name",
            F.date_format(
                F.col("date"),
                "MMM"
            )
        )

        .withColumn(
            "year_month",
            F.date_format(
                F.col("date"),
                "yyyy-MM"
            )
        )

        .withColumn(
            "week_of_year",
            F.weekofyear("date")
        )

        .withColumn(
            "day_of_month",
            F.dayofmonth("date")
        )

        .withColumn(
            "day_of_week",
            F.dayofweek("date")
        )

        .withColumn(
            "day_name",
            F.date_format(
                F.col("date"),
                "EEEE"
            )
        )

        .withColumn(
            "day_short_name",
            F.date_format(
                F.col("date"),
                "EEE"
            )
        )

        .withColumn(
            "is_weekend",
            F.dayofweek("date").isin(1, 7)
        )

        .select(
            "date_key",
            "date",
            "year",
            "quarter",
            "quarter_name",
            "month",
            "month_name",
            "month_short_name",
            "year_month",
            "week_of_year",
            "day_of_month",
            "day_of_week",
            "day_name",
            "day_short_name",
            "is_weekend"
        )
    )

    # --------------------------------------------------------
    # 5. WRITE GOLD DIMENSION
    # --------------------------------------------------------

    (
        dim_date.write
        .mode("overwrite")
        .parquet(
            str(GOLD_DATE_PATH)
        )
    )

    print(
        f"\nGold dim_date written to:\n"
        f"{GOLD_DATE_PATH}"
    )

    # --------------------------------------------------------
    # 6. VALIDATION
    # --------------------------------------------------------

    print("\n" + "=" * 80)
    print("GOLD DIM_DATE VALIDATION")
    print("=" * 80)

    print(
        f"Rows: {dim_date.count()}"
    )

    print("\nSchema:")

    dim_date.printSchema()

    print("\nSample records:")

    dim_date.show(
        10,
        truncate=False
    )

    # --------------------------------------------------------
    # 7. CHECK DATE KEY UNIQUENESS
    # --------------------------------------------------------

    duplicate_date_keys = (
        dim_date
        .groupBy("date_key")
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    print(
        f"Duplicate date_key values: "
        f"{duplicate_date_keys}"
    )

    # --------------------------------------------------------
    # 8. CHECK NULL KEYS
    # --------------------------------------------------------

    null_date_keys = (
        dim_date
        .filter(
            F.col("date_key").isNull()
        )
        .count()
    )

    print(
        f"Null date_key values: "
        f"{null_date_keys}"
    )

    # --------------------------------------------------------
    # 9. COMPLETION
    # --------------------------------------------------------

    print("\n" + "=" * 80)
    print(
        "GOLD DIM_DATE COMPLETED SUCCESSFULLY"
    )
    print("=" * 80)

    spark.stop()