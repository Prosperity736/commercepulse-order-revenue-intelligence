from pathlib import Path

from pyspark.sql import functions as F

from src.ingestion.spark_session import create_spark_session


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_ORDER_REVIEWS_PATH = (
    PROJECT_ROOT
    / "data"
    / "bronze"
    / "order_reviews"
)

SILVER_ORDER_REVIEWS_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "order_reviews"
)


# ============================================================
# MAIN TRANSFORMATION
# ============================================================

if __name__ == "__main__":

    spark = create_spark_session()

    # --------------------------------------------------------
    # 1. READ BRONZE ORDER REVIEWS
    # --------------------------------------------------------

    reviews_df = spark.read.parquet(
        str(BRONZE_ORDER_REVIEWS_PATH)
    )

    print("Bronze order reviews loaded.")

    print(
        f"Bronze rows: "
        f"{reviews_df.count()}"
    )

    reviews_df.printSchema()


    # --------------------------------------------------------
    # 2. CLEAN STRING COLUMNS
    # --------------------------------------------------------

    silver_reviews_df = (
        reviews_df

        # Review ID
        .withColumn(
            "review_id",
            F.trim(F.col("review_id"))
        )

        # Order ID
        .withColumn(
            "order_id",
            F.trim(F.col("order_id"))
        )

        # Review score
        #
        # IMPORTANT:
        # Use try_cast instead of cast because the Bronze
        # data may contain malformed values.
        #
        .withColumn(
            "review_score",
            F.expr(
                "try_cast(trim(review_score) AS INT)"
            )
        )

        # Review comment title
        .withColumn(
            "review_comment_title",
            F.when(
                F.trim(
                    F.col("review_comment_title")
                ) == "",
                None
            ).otherwise(
                F.trim(
                    F.col("review_comment_title")
                )
            )
        )

        # Review comment message
        .withColumn(
            "review_comment_message",
            F.when(
                F.trim(
                    F.col("review_comment_message")
                ) == "",
                None
            ).otherwise(
                F.trim(
                    F.col("review_comment_message")
                )
            )
        )

        # ----------------------------------------------------
        # 3. CONVERT REVIEW CREATION DATE
        # ----------------------------------------------------
        #
        # try_cast prevents malformed values from crashing
        # the entire Spark job.
        #
        .withColumn(
            "review_creation_date",
            F.expr(
                "try_cast(trim(review_creation_date) AS TIMESTAMP)"
            )
        )

        # ----------------------------------------------------
        # 4. CONVERT REVIEW ANSWER TIMESTAMP
        # ----------------------------------------------------

        .withColumn(
            "review_answer_timestamp",
            F.expr(
                "try_cast(trim(review_answer_timestamp) AS TIMESTAMP)"
            )
        )
    )


    # --------------------------------------------------------
    # 5. VALIDATE REVIEW SCORE
    # --------------------------------------------------------

    silver_reviews_df = (
        silver_reviews_df
        .filter(
            F.col("review_score").between(1, 5)
        )
    )


    # --------------------------------------------------------
    # 6. REMOVE INVALID RECORDS
    # --------------------------------------------------------

    silver_reviews_df = (
        silver_reviews_df
        .filter(
            F.col("review_id").isNotNull()
        )
        .filter(
            F.col("order_id").isNotNull()
        )
    )


    # --------------------------------------------------------
    # 7. REMOVE DUPLICATE REVIEWS
    # --------------------------------------------------------

    silver_reviews_df = (
        silver_reviews_df
        .dropDuplicates(
            ["review_id"]
        )
    )


    # --------------------------------------------------------
    # 8. SELECT FINAL SILVER COLUMNS
    # --------------------------------------------------------

    silver_reviews_df = silver_reviews_df.select(
        "review_id",
        "order_id",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_creation_date",
        "review_answer_timestamp"
    )


    # --------------------------------------------------------
    # 9. SHOW TRANSFORMATION RESULTS
    # --------------------------------------------------------

    print(
        "Silver order reviews transformation completed!"
    )

    print(
        f"Number of rows: "
        f"{silver_reviews_df.count()}"
    )

    print("\nSilver schema:")

    silver_reviews_df.printSchema()

    print("\nSample Silver records:")

    silver_reviews_df.show(
        5,
        truncate=False
    )


    # --------------------------------------------------------
    # 10. WRITE SILVER DATA
    # --------------------------------------------------------

    (
        silver_reviews_df.write
        .mode("overwrite")
        .parquet(
            str(SILVER_ORDER_REVIEWS_PATH)
        )
    )

    print(
        f"\nSilver order reviews written to: "
        f"{SILVER_ORDER_REVIEWS_PATH}"
    )


    # --------------------------------------------------------
    # 11. STOP SPARK
    # --------------------------------------------------------

    spark.stop()