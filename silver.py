"""Bronze -> silver for one month of NYC yellow-taxi data.

Rows failing a quality rule go to quarantine with a reason; the rest go to silver.
Idempotent: re-running a month overwrites only that month's output.
Usage: python silver.py 2025-01
"""
from __future__ import annotations

import argparse
import logging
import re
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

log = logging.getLogger("silver")
MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def month_bounds(month: str) -> tuple[str, str]:
    """Return (first day of month, first day of next month) as ISO dates."""
    year, mm = int(month[:4]), int(month[5:])
    start = date(year, mm, 1)
    end = date(year + 1, 1, 1) if mm == 12 else date(year, mm + 1, 1)
    return start.isoformat(), end.isoformat()


def tag_rejects(df: DataFrame, month: str) -> DataFrame:
    """Add reject_reason: null means a clean row. The first matching rule wins."""
    start, end = month_bounds(month)
    pickup = F.col("tpep_pickup_datetime")
    dropoff = F.col("tpep_dropoff_datetime")
    reason = (
        F.when((pickup < start) | (pickup >= end), "pickup_outside_month")
        .when(dropoff < pickup, "dropoff_before_pickup")
        .when(F.col("fare_amount") < 0, "negative_fare")
        .when(F.col("trip_distance") <= 0, "non_positive_distance")
    )
    return df.withColumn("reject_reason", reason)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("month", help="YYYY-MM, e.g. 2025-01")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if not MONTH_RE.match(args.month):
        log.error("Invalid month %r, expected YYYY-MM", args.month)
        return 2

    spark = SparkSession.builder.master("local[2]").appName("silver").getOrCreate()
    try:
        bronze = spark.read.parquet(f"data/yellow_tripdata_{args.month}.parquet")
        tagged = tag_rejects(bronze, args.month).cache()  # reused several times below

        silver = tagged.filter(F.col("reject_reason").isNull()).drop("reject_reason")
        quarantine = tagged.filter(F.col("reject_reason").isNotNull())

        bronze_n, silver_n, quarantine_n = bronze.count(), silver.count(), quarantine.count()
        log.info("bronze=%s silver=%s quarantine=%s", f"{bronze_n:,}", f"{silver_n:,}", f"{quarantine_n:,}")
        if bronze_n != silver_n + quarantine_n:
            raise RuntimeError("Row counts do not reconcile: rows were lost or duplicated")

        silver.write.mode("overwrite").parquet(f"data/silver/yellow/{args.month}")
        quarantine.write.mode("overwrite").parquet(f"data/quarantine/yellow/{args.month}")
        return 0
    finally:
        spark.stop()  # runs even if something above failed


if __name__ == "__main__":
    raise SystemExit(main())
