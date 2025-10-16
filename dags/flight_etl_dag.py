from datetime import datetime
from pathlib import Path
import importlib.util
from typing import Iterable

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SRC_DIR = PROJECT_ROOT / "src"
_RAW_FALLBACKS = (
    DEFAULT_SRC_DIR,
    PROJECT_ROOT / "dags" / "src",
    Path("/opt/airflow/src"),
    Path("/opt/airflow/dags/src"),
)
_unique_fallbacks = []
for candidate in _RAW_FALLBACKS:
    if candidate not in _unique_fallbacks:
        _unique_fallbacks.append(candidate)
SRC_DIR_FALLBACKS: Iterable[Path] = tuple(_unique_fallbacks)


def _load_module(module_name: str, relative_path: Path):
    for base_dir in SRC_DIR_FALLBACKS:
        module_path = base_dir / relative_path
        if module_path.exists():
            spec = importlib.util.spec_from_file_location(module_name, module_path)
            if spec is None or spec.loader is None:
                continue
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module
    raise FileNotFoundError(
        f"Unable to locate {relative_path} in any of: "
        + ", ".join(str(path) for path in SRC_DIR_FALLBACKS)
    )


_extract = _load_module("flight_pipeline.extract", Path("extract") / "flights.py")
_transform = _load_module(
    "flight_pipeline.transform", Path("transform") / "transform_flights.py"
)
_utils = _load_module("flight_pipeline.utils", Path("utils") / "io.py")

fetch_flights_data = _extract.fetch_flights_data
save_raw_data = _extract.save_raw_data
transform_flight_data = _transform.transform_flight_data
upload_to_azure = _utils.upload_to_azure

RAW_DATA_DIR = Path("data/raw/flights")
PROCESSED_DATA_DIR = Path("data/processed/flights")


def extract_opensky_data():
    df = fetch_flights_data()
    if df.empty:
        raise ValueError("OpenSky API returned no data.")
    save_raw_data(df)


def load_latest_file(base_dir: Path, prefix: str) -> str:
    if not base_dir.exists():
        raise FileNotFoundError(f"Directory {base_dir} does not exist.")
    parquet_files = sorted(base_dir.glob(f"{prefix}*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {base_dir}")
    return str(parquet_files[-1])


def load_raw():
    latest_raw = load_latest_file(RAW_DATA_DIR, "flights_raw_")
    upload_to_azure(latest_raw)


def load_processed():
    latest_processed = load_latest_file(PROCESSED_DATA_DIR, "processed_flights_raw_")
    upload_to_azure(latest_processed)


with DAG(
    dag_id="flight_etl_dag",
    start_date=datetime(2025, 10, 10),
    schedule_interval="@daily",
    catchup=False,
    default_args={"owner": "kaancakir", "retries": 1},
    description="End-to-end Flight ETL pipeline (OpenSky → ADLS)",
) as dag:
    extract_task = PythonOperator(
        task_id="extract_raw_flight_data",
        python_callable=extract_opensky_data,
    )

    transform_task = PythonOperator(
        task_id="transform_flight_data",
        python_callable=transform_flight_data,
    )

    load_raw_task = PythonOperator(
        task_id="load_raw_to_adls",
        python_callable=load_raw,
    )

    load_processed_task = PythonOperator(
        task_id="load_processed_to_adls",
        python_callable=load_processed,
    )

    extract_task >> transform_task >> [load_raw_task, load_processed_task]
