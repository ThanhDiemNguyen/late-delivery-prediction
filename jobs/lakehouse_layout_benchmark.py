"""Lakehouse layout benchmark: small files, then OPTIMIZE, then Z-order, measured by two queries.

    spark-submit /opt/jobs/lakehouse_layout_benchmark.py --days 7
    spark-submit /opt/jobs/lakehouse_layout_benchmark.py --silver   # the pipeline's own table, once

One week of full-scale pings goes into a Delta table partitioned by ping_date, written after a
200-way random shuffle, as a job with many tasks writes it: each task writes a file into every
date, so each date holds about 200 small files. Then:

    optimize   OPTIMIZE compacts each date into files of up to 128 MB
    zorder     OPTIMIZE ZORDER BY (courier_id) rewrites them so each file holds a narrow range of
               couriers, and Delta's per-file min/max statistics let a query skip the other files
    vacuum     VACUUM deletes the files the two rewrites replaced

After each step two queries run twice (the second run counts): one courier on one day, and one hub
over the week. Their files read, bytes read and time come from the Spark SQL metrics of the scan.
The table lives at s3a://lakehouse/_benchmarks/layout/pings; the results go next to it.

--silver measures silver.stg_courier_pings itself, before and after one OPTIMIZE ZORDER BY
(courier_id) of all its dates; from then on DP2 compacts each date it writes.
"""

import argparse
import json
import time
from datetime import date, timedelta
from urllib.request import urlopen

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F

from common import lakehouse_session

PINGS = "s3a://app-exports/courier_pings"
TABLE = "s3a://lakehouse/_benchmarks/layout/pings"
FIRST_DAY = date(2024, 6, 1)
QUERIES = {
    "one courier, one day": "SELECT count(*), round(avg(speed_kmh), 2) FROM delta.`{t}` WHERE ping_date = '2024-06-03' AND courier_id = {courier}",
    "one hub, one week": "SELECT ping_date, count(*) FROM delta.`{t}` WHERE hub_id = 17 GROUP BY ping_date",
}


def scan_metrics(spark: SparkSession) -> dict:
    """Return files read, bytes read and scan time of the last query's scan of the data files.

    Delta also runs small queries of its own (reading the transaction log, a "Scan json"), so this
    takes the most recent execution with a "Scan parquet" node over the table's data.
    """
    base = f"{spark.sparkContext.uiWebUrl}/api/v1/applications/{spark.sparkContext.applicationId}"
    for execution in sorted(json.load(urlopen(f"{base}/sql?details=true&length=1000")), key=lambda e: -e["id"]):
        scans = [n for n in execution["nodes"] if n["nodeName"] == "Scan parquet"]
        if scans:
            metrics = {m["name"]: m["value"] for m in scans[0]["metrics"]}
            return {k: metrics.get(k) for k in ("number of files read", "size of files read", "number of output rows")}
    return {}


def measure(spark: SparkSession, step: str, courier: int, table: str = TABLE, queries: dict = QUERIES) -> dict:
    """Return the table's file count and size and each query's scan metrics and time."""
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{table}`").first()
    result = {"step": step, "files": detail.numFiles, "gb": round(detail.sizeInBytes / 1e9, 3), "queries": {}}
    for name, sql in queries.items():
        for _ in range(2):
            started = time.perf_counter()
            spark.sql(sql.format(t=table, courier=courier)).collect()
            seconds = round(time.perf_counter() - started, 2)
        result["queries"][name] = {"seconds": seconds, **scan_metrics(spark)}
    print(json.dumps(result))
    return result


def silver(spark: SparkSession) -> None:
    """Measure silver.stg_courier_pings, compact and z-order all its dates once, and measure again."""
    path = "s3a://lakehouse/silver/stg_courier_pings"
    queries = {
        "one courier, one day": QUERIES["one courier, one day"],
        "one hub, one week": "SELECT ping_date, count(*) FROM delta.`{t}` WHERE hub_id = 17 "
                             "AND ping_date BETWEEN '2024-06-01' AND '2024-06-07' GROUP BY ping_date",
    }
    courier = spark.read.format("delta").load(path).where("ping_date = '2024-06-03'").select("courier_id").first()[0]
    before = measure(spark, "silver before", courier, path, queries)
    started = time.perf_counter()
    DeltaTable.forPath(spark, path).optimize().executeZOrderBy("courier_id")
    seconds = round(time.perf_counter() - started, 1)
    after = measure(spark, "silver after OPTIMIZE ZORDER BY (courier_id)", courier, path, queries)
    print(json.dumps({"silver": True, "courier": courier, "zorder_s": seconds, "steps": [before, after]})[:200])


def main() -> None:
    """Write the table, measure it, then optimize, z-order and vacuum it, measuring after each."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--silver", action="store_true", help="measure and compact silver.stg_courier_pings instead")
    args = parser.parse_args()
    spark = lakehouse_session("lakehouse_layout_benchmark")
    if args.silver:
        silver(spark)
        spark.stop()
        return
    spark.conf.set("spark.databricks.delta.optimize.maxFileSize", str(128 * 1024 * 1024))
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    dates = [FIRST_DAY + timedelta(d) for d in range(args.days)]
    pings = spark.read.parquet(*[f"{PINGS}/export_date={d}" for d in dates]).withColumn(
        "ping_date", F.to_date(F.from_utc_timestamp("ping_ts", "America/Los_Angeles")))
    steps, timings = [], {}

    started = time.perf_counter()
    pings.repartition(200).write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .partitionBy("ping_date").save(TABLE)
    timings["write"] = round(time.perf_counter() - started, 1)
    courier = spark.read.format("delta").load(TABLE).where("ping_date = '2024-06-03'").select("courier_id").first()[0]
    steps.append(measure(spark, "after the write", courier))

    table = DeltaTable.forPath(spark, TABLE)
    started = time.perf_counter()
    table.optimize().executeCompaction()
    timings["optimize"] = round(time.perf_counter() - started, 1)
    steps.append(measure(spark, "after OPTIMIZE", courier))

    started = time.perf_counter()
    table.optimize().executeZOrderBy("courier_id")
    timings["zorder"] = round(time.perf_counter() - started, 1)
    steps.append(measure(spark, "after ZORDER BY (courier_id)", courier))

    started = time.perf_counter()
    table.vacuum(0)
    timings["vacuum"] = round(time.perf_counter() - started, 1)
    steps.append(measure(spark, "after VACUUM", courier))

    report = {"days": args.days, "courier": courier, "rows": spark.read.format("delta").load(TABLE).count(), "timings_s": timings, "steps": steps}
    spark.createDataFrame([(json.dumps(report),)], "json string").coalesce(1).write.mode("overwrite").text(
        "s3a://lakehouse/_benchmarks/layout/result.json")
    print(json.dumps({k: v for k, v in report.items() if k != "steps"}))
    spark.stop()


if __name__ == "__main__":
    main()
