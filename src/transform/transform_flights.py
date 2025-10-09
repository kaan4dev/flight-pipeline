import pandas as pd
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round

def main():
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

    print(f"✅ Transformation complete. Saved to: {out_path}")

    spark.stop()


if __name__ == "__main__":
    main()