import requests
import pandas as pd
import os
from datetime import datetime
import logging

logging.basicConfig(
    filename="logs/extract_flights.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

API_URL = "https://opensky-network.org/api/states/all"

def fetch_flights_data(limit=200):

    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        data = response.json().get("states", [])

        columns = [
            "icao24", "callsign", "origin_country", "time_position", "last_contact",
            "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
            "true_track", "vertical_rate", "sensors", "geo_altitude",
            "squawk", "spi", "position_source"
        ]

        df = pd.DataFrame(data, columns=columns)
        df = df.head(limit)
        return df

    except Exception as e:
        logging.error(f"API request failed: {e}")
        print(f"OpenSky API request failed: {e}")
        return pd.DataFrame()


def save_raw_data(df):
    os.makedirs("data/raw/flights", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = f"data/raw/flights/flights_raw_{timestamp}.parquet"
    df.to_parquet(path, index=False)
    logging.info(f"Saved raw data to {path}")
    print(f"Saved raw data: {path}")


def main():
    print("Fetching live flight data from OpenSky...")
    df = fetch_flights_data(limit=200)

    if not df.empty:
        save_raw_data(df)
    else:
        print("No data fetched (empty response).")

    logging.info("Flight extraction completed.")


if __name__ == "__main__":
    main()
