from pathlib import Path

from src.ingestion.spark_session import create_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SILVER_GEOLOCATION_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "geolocation"
)


def test_silver_geolocation():

    spark = create_spark_session()

    df = spark.read.parquet(
        str(SILVER_GEOLOCATION_PATH)
    )

    # Expected row count
    row_count = df.count()

    assert row_count == 1000163, (
        f"Expected 1000163 rows, got {row_count}"
    )

    # Expected columns
    expected_columns = [
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng",
        "geolocation_city",
        "geolocation_state",
    ]

    assert df.columns == expected_columns, (
        f"Unexpected columns: {df.columns}"
    )

    # ZIP code should not be negative
    negative_zip_codes = df.filter(
        df.geolocation_zip_code_prefix < 0
    ).count()

    assert negative_zip_codes == 0, (
        "geolocation_zip_code_prefix contains negative values"
    )

    # Latitude must not be NULL
    null_latitudes = df.filter(
        df.geolocation_lat.isNull()
    ).count()

    assert null_latitudes == 0, (
        "geolocation_lat contains NULL values"
    )

    # Longitude must not be NULL
    null_longitudes = df.filter(
        df.geolocation_lng.isNull()
    ).count()

    assert null_longitudes == 0, (
        "geolocation_lng contains NULL values"
    )

    print("✓ Silver geolocation validation passed")
    print(f"✓ Rows: {row_count}")
    print(f"✓ Columns: {len(df.columns)}")
    print("✓ No negative ZIP codes")
    print("✓ geolocation_lat contains no NULL values")
    print("✓ geolocation_lng contains no NULL values")

    spark.stop()


if __name__ == "__main__":
    test_silver_geolocation()