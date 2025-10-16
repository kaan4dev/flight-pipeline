import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round

from src.quality.quality_checks import run_basic_checks  # noqa: E402
from src.utils.io import upload_processed_to_azure  # noqa: E402


def transform_flight_data():
    spark = SparkSession.builder.appName("FlightTransform_OpenSky").getOrCreate()

    RAW_DIR = "data/raw/flights"
    PROCESSED_DIR = "data/processed/flights"

    latest_file = sorted(os.listdir(RAW_DIR))[-1]
    raw_path = os.path.join(RAW_DIR, latest_file)
    out_path = os.path.join(PROCESSED_DIR, f"processed_{latest_file}")

    print(f"Reading from: {raw_path}")
    df = spark.read.parquet(raw_path)

    print("Iinitial Schema: ")
    df.printSchema()

    df_clean = df.filter(
        (col("callsign").isNotNull()) &
        (col("longitude").isNotNull()) &
        (col("latitude").isNotNull())
    )

    df_clean = df_clean.withColumn("velocity_kmh", spark_round(col("velocity")*3.6,2))

    avg_altitude = df_clean.select("baro_altitude").na.drop().agg({"baro_altitude": "avg"}).collect()[0][0]
    print(f"Average barometric altitude: {avg_altitude:.2f} m")

    df_clean = df_clean.na.drop(subset=["longitude", "latitude", "origin_country"])

    os.makedirs(PROCESSED_DIR, exist_ok=True)
    df_clean.write.mode("overwrite").parquet(out_path)

    summary = run_basic_checks(df_clean)

    if summary["status"] == "FAIL":
        raise RuntimeError("Data quality FAIL: empty dataset or severe anomalies detected.")

    upload_processed_to_azure(out_path)

    print(f"Transformation complete. Saved to: {out_path}")

    spark.stop()


if __name__ == "__main__":
    transform_flight_data()
