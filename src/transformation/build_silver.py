import subprocess
import sys


SILVER_TRANSFORMATIONS = [
    "src.transformation.silver_customers",
    "src.transformation.silver_geolocation",
    "src.transformation.silver_orders",
    "src.transformation.silver_order_items",
    "src.transformation.silver_order_payments",
    "src.transformation.silver_order_reviews",
    "src.transformation.silver_products",
    "src.transformation.silver_sellers",
    "src.transformation.silver_product_category_translation",
]


def run_transformation(module):
    print("=" * 70)
    print(f"Running: {module}")
    print("=" * 70)

    result = subprocess.run(
        [sys.executable, "-m", module],
        check=True
    )

    return result


if __name__ == "__main__":

    print("\nStarting Silver transformation pipeline...\n")

    for module in SILVER_TRANSFORMATIONS:
        run_transformation(module)

    print("\n" + "=" * 70)
    print("Silver transformation pipeline completed successfully!")
    print("=" * 70)