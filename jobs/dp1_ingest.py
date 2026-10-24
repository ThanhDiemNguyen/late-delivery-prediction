"""DP1: copy both source systems into bronze Delta tables, unchanged and incrementally.

    oms.<table> (Postgres, JDBC)                     -> bronze.raw_<table>         rows changed since the last run
    s3a://app-exports/courier_pings/export_date=*/   -> bronze.raw_courier_pings   export dates not loaded yet
    s3a://app-exports/parcel_scans/export_date=*/    -> bronze.raw_parcel_scans    export dates not loaded yet

Bronze keeps every source problem: legacy spellings, NaN literals, broken coordinates, re-uploaded
pings, two ping schemas. Silver fixes them. Every row gets _ingested_at and _source.

Usage, inside spark-master:
    spark-submit /opt/jobs/dp1_ingest.py                      # everything new
    spark-submit /opt/jobs/dp1_ingest.py --until 2024-07-31   # only up to a date (incremental demo, Airflow)
    spark-submit /opt/jobs/dp1_ingest.py --part exports       # one part only: oms | exports
    spark-submit /opt/jobs/dp1_ingest.py --no-merge-schema    # show what happens without mergeSchema

The Postgres login comes from POSTGRES_USER and POSTGRES_PASSWORD, and S3 keys from AWS_*.
"""

import argparse
import os

from pyspark.sql import DataFrame, SparkSession, functions as F

from common import EXPORTS, LAKEHOUSE, lakehouse_session

BRONZE = f"{LAKEHOUSE}/bronze"
OMS_URL = os.environ.get("OMS_JDBC_URL", "jdbc:postgresql://postgres:5432/oms")
# Each oms table and the column that tells which rows changed since the last run.
OMS_TABLES = {
    "hubs": ("updated_ts", "hub_id"),
    "couriers": ("updated_ts", "courier_id"),
    "courier_changes": ("changed_ts", "change_id"),
    "customers": ("updated_ts", "customer_id"),
    "orders": ("updated_ts", "order_id"),
}
EXPORT_DATASETS = ("courier_pings", "parcel_scans")
JDBC_PARTITIONS_FROM_ROWS = 1_000_000  # read bigger windows in parallel, split by the key
MAX_RECORDS_PER_FILE = 5_000_000


def save(df: DataFrame, table: str, partition_by: str | None = None, merge_schema: bool = True) -> None:
    """Append df to the Delta table at BRONZE/<table> and register it as bronze.<table> once.

    The table is written by path and registered with CREATE TABLE IF NOT EXISTS, so later
    schema changes live in the Delta log and never rewrite the metastore entry.
    """
    path = f"{BRONZE}/{table}"
    writer = df.write.format("delta").mode("append").option("mergeSchema", str(merge_schema).lower())
    writer = writer.option("maxRecordsPerFile", MAX_RECORDS_PER_FILE)
    (writer.partitionBy(partition_by) if partition_by else writer).save(path)
    df.sparkSession.sql(f"CREATE TABLE IF NOT EXISTS bronze.{table} USING DELTA LOCATION '{path}'")


def _jdbc(spark: SparkSession, query: str, **options) -> DataFrame:
    """Read a SQL query from oms."""
    return (
        spark.read.format("jdbc")
        .options(url=OMS_URL, dbtable=f"({query}) AS q", driver="org.postgresql.Driver", fetchsize="10000", **options)
        .option("user", os.environ["POSTGRES_USER"])
        .option("password", os.environ["POSTGRES_PASSWORD"])
        .load()
    )


def ingest_oms(spark: SparkSession, until: str | None) -> None:
    """Append the rows of each oms table whose change column is after the bronze watermark (and up to until).

    Bronze appends every version it pulls; silver keeps the latest version of each key.
    """
    for table, (changed, key) in OMS_TABLES.items():
        bronze = f"bronze.raw_{table}"
        watermark = None
        if spark.catalog.tableExists(bronze):
            # Formatted by Spark in the session time zone (UTC), not by Python in the process's zone.
            watermark = spark.table(bronze).agg(F.date_format(F.max(changed), "yyyy-MM-dd HH:mm:ss.SSSSSS")).first()[0]
        conditions = []
        if watermark is not None:
            conditions.append(f"{changed} > TIMESTAMPTZ '{watermark}+00:00'")
        if until:
            conditions.append(f"{changed} < (DATE '{until}' + 1)::timestamp AT TIME ZONE 'America/Los_Angeles'")
        where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        bounds = _jdbc(spark, f"SELECT count(*) AS n, min({key}) AS lo, max({key}) AS hi FROM {table}{where}").first()
        if not bounds.n:
            print(f"{bronze}: nothing changed after {watermark}")
            continue
        parallel = {}
        if bounds.n >= JDBC_PARTITIONS_FROM_ROWS:
            parallel = dict(partitionColumn=key, lowerBound=str(bounds.lo), upperBound=str(bounds.hi + 1), numPartitions="8")
        rows = (
            _jdbc(spark, f"SELECT * FROM {table}{where}", **parallel)
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source", F.lit(f"postgres:oms.{table}"))
        )
        save(rows, f"raw_{table}")
        print(f"{bronze}: +{bounds.n:,} rows changed after {watermark}; {spark.table(bronze).count():,} rows in total")


def export_dates(spark: SparkSession, dataset: str) -> list[str]:
    """Return the export_date folders of a dataset that are complete (have _SUCCESS), oldest first."""
    jvm = spark._jvm
    root = jvm.org.apache.hadoop.fs.Path(f"{EXPORTS}/{dataset}")
    fs = root.getFileSystem(spark._jsc.hadoopConfiguration())
    folders = [s.getPath() for s in fs.listStatus(root) if s.isDirectory()]
    done = [p.getName().split("=", 1)[1] for p in folders if fs.exists(jvm.org.apache.hadoop.fs.Path(p, "_SUCCESS"))]
    return sorted(done)


def ingest_exports(spark: SparkSession, until: str | None, merge_schema: bool) -> None:
    """Append the complete export dates after the newest one already in bronze (and up to until).

    From 2024-09-01 a ping folder holds v1 and v2 files. With mergeSchema the read takes the union
    of both schemas and the Delta append adds the v2 columns; without it Spark keeps one file's
    schema and silently drops the other's columns.
    """
    for dataset in EXPORT_DATASETS:
        bronze = f"bronze.raw_{dataset}"
        done = str(spark.table(bronze).agg(F.max("export_date")).first()[0]) if spark.catalog.tableExists(bronze) else ""
        new = [d for d in export_dates(spark, dataset) if d > done and (until is None or d <= until)]
        if not new:
            print(f"{bronze}: no complete export date after {done or 'the start'}")
            continue
        base = f"{EXPORTS}/{dataset}"
        rows = (
            spark.read.options(basePath=base, mergeSchema=str(merge_schema).lower())
            .parquet(*[f"{base}/export_date={d}" for d in new])
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source", F.col("_metadata.file_path"))
        )
        save(rows.repartition("export_date"), f"raw_{dataset}", partition_by="export_date", merge_schema=merge_schema)
        print(f"{bronze}: +{len(new)} export dates ({new[0]} to {new[-1]}); {spark.table(bronze).count():,} rows in total")


def main() -> None:
    """Parse the options and run the requested parts."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--part", choices=["oms", "exports"], help="run one part only")
    parser.add_argument("--until", help="last local date to load, YYYY-MM-DD")
    parser.add_argument("--no-merge-schema", action="store_true", help="read and append exports without mergeSchema")
    args = parser.parse_args()
    spark = lakehouse_session("dp1_ingest")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS bronze LOCATION '{BRONZE}'")
    if args.part in (None, "oms"):
        ingest_oms(spark, args.until)
    if args.part in (None, "exports"):
        ingest_exports(spark, args.until, merge_schema=not args.no_merge_schema)
    spark.stop()


if __name__ == "__main__":
    main()
