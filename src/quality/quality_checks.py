"""Data quality checks for transformed flight data."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


DEFAULT_THRESHOLDS: Dict[str, float] = {
    "max_velocity_kmh": 1500.0,
    "max_baro_altitude": 20000.0,
    "min_rows": 1,
}


def _ensure_directory(path: Path) -> None:
    """Create the directory if it does not already exist."""

    path.mkdir(parents=True, exist_ok=True)


def run_basic_checks(
    df: DataFrame,
    thresholds: Optional[Dict[str, float]] = None,
    report_dir: str = "data/reports",
) -> Dict[str, float]:
    """Run sanity checks on the transformed dataset and persist a JSON report."""

    merged_thresholds = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    report_path = Path(report_dir)
    _ensure_directory(report_path)

    total_rows = df.count()
    null_longitude = df.filter(col("longitude").isNull()).count()
    null_latitude = df.filter(col("latitude").isNull()).count()
    null_country = df.filter(col("origin_country").isNull()).count()
    negative_speed = df.filter(col("velocity_kmh") < 0).count()
    overspeed = df.filter(col("velocity_kmh") > merged_thresholds["max_velocity_kmh"]).count()
    over_altitude = df.filter(col("baro_altitude") > merged_thresholds["max_baro_altitude"]).count()

    summary = {
        "timestamp": datetime.utcnow().isoformat(),
        "row_count": total_rows,
        "null_longitude": null_longitude,
        "null_latitude": null_latitude,
        "null_origin_country": null_country,
        "negative_speed_rows": negative_speed,
        "overspeed_rows": overspeed,
        "over_altitude_rows": over_altitude,
        "thresholds": merged_thresholds,
        "status": "PASS",
    }

    if total_rows < merged_thresholds["min_rows"]:
        summary["status"] = "FAIL"
    elif negative_speed or overspeed or over_altitude:
        summary["status"] = "WARN"

    filename = f"quality_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    report_file = report_path / filename

    with report_file.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)

    print(f"Quality report saved: {report_file}")
    print(f"Quality status: {summary['status']}")
    return summary


__all__ = ["run_basic_checks", "DEFAULT_THRESHOLDS"]
