"""Dedup benchmark: two ways to drop the re-uploaded pings (2% of the rows) from the exports.

    spark-submit /opt/jobs/dedup_benchmark.py --method window --days 7
    spark-submit /opt/jobs/dedup_benchmark.py --method targeted --days 7

A re-upload repeats (courier_id, device_seq) with a later uploaded_ts, in the same export_date
folder. Both methods keep the earliest upload of each key:

    window     what silver does: row_number() over (courier_id, device_seq) ordered by uploaded_ts,
               which shuffles and sorts every column of every row
    targeted   one export_date at a time: find the duplicated keys from the two key columns only,
               let every other row through a broadcast anti-join (no shuffle of the wide rows), and
               run the window only on the rows of the duplicated keys

Both report the rows in and out, a checksum of the kept rows (they must agree), the wall time, and
the shuffle and spill of every stage. The result goes to s3a://lakehouse/_benchmarks/dedup/.
"""

import argparse
import json
import time
from datetime import date, timedelta

from pyspark.sql import DataFrame, Window, functions as F

from common import lakehouse_session, stage_metrics

PINGS = "s3a://app-exports/courier_pings"
OUT = "s3a://lakehouse/_benchmarks/dedup"
KEY = ["courier_id", "device_seq"]
FIRST_DAY = date(2024, 6, 1)


def earliest_upload(df: DataFrame) -> DataFrame:
    """Keep the earliest upload of each key."""
    first = Window.partitionBy(*KEY).orderBy("uploaded_ts")
    return df.withColumn("n", F.row_number().over(first)).where(F.col("n") == 1).drop("n")


def targeted(day: DataFrame) -> DataFrame:
    """Dedup one export date: only the rows of duplicated keys go through the window."""
    duplicated = day.groupBy(*KEY).count().where(F.col("count") > 1).select(*KEY)
    keys = F.broadcast(duplicated)
    return day.join(keys, KEY, "left_anti").unionByName(earliest_upload(day.join(keys, KEY, "left_semi")))


def summary(df: DataFrame) -> dict:
    """Return the row count and an order-free checksum of the kept rows."""
    row = df.agg(F.count("*").alias("rows"), F.sum(F.xxhash64(*KEY, "uploaded_ts")).alias("hash")).first()
    return {"rows": row.rows, "hash": row.hash}


def main() -> None:
    """Run one method over the first N export dates and record the result."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--method", choices=["window", "targeted"], required=True)
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args()
    spark = lakehouse_session(f"dedup_{args.method}")
    days = [FIRST_DAY + timedelta(d) for d in range(args.days)]
    read = lambda d: spark.read.parquet(f"{PINGS}/export_date={d}")  # noqa: E731  (whole rows, as silver reads them)

    started = time.perf_counter()
    rows_in = sum(read(d).count() for d in days)
    counted = time.perf_counter() - started
    started = time.perf_counter()
    if args.method == "window":
        kept = summary(earliest_upload(spark.read.parquet(*[f"{PINGS}/export_date={d}" for d in days])))
    else:
        parts = [summary(targeted(read(d))) for d in days]
        kept = {"rows": sum(p["rows"] for p in parts), "hash": sum(p["hash"] for p in parts)}
        kept["hash"] = (kept["hash"] + 2**63) % 2**64 - 2**63  # Spark's sum of longs wraps the same way
    seconds = round(time.perf_counter() - started, 1)
    removed = rows_in - kept["rows"]
    report = {
        "method": args.method, "days": args.days, "rows_in": rows_in, "rows_kept": kept["rows"], "removed": removed,
        "removed_pct_of_kept": round(100 * removed / kept["rows"], 4), "hash": kept["hash"], "seconds": seconds,
        "count_seconds": round(counted, 1), "app_id": spark.sparkContext.applicationId, "stages": stage_metrics(spark),
    }
    spark.createDataFrame([(json.dumps(report),)], "json string").coalesce(1).write.mode("overwrite").text(
        f"{OUT}/{args.method}-{args.days}d.json")
    print(json.dumps({k: v for k, v in report.items() if k != "stages"}))
    totals = {k: round(sum(s[k] for s in report["stages"]), 3) for k in ("shuffle_write_gb", "spill_memory_gb", "spill_disk_gb")}
    print(json.dumps({"totals": totals}))
    spark.stop()


if __name__ == "__main__":
    main()
