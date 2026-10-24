"""Schema evolution demo: the courier app's v2 pings next to v1 pings, read and stored three ways.

    spark-submit /opt/jobs/schema_evolution_demo.py

From 2024-09-01 the app v2 rolls out over 14 days. v2 adds battery_pct and gps_accuracy_m and
replaces speed_kmh (int) with speed_mps (float), in the same export_date folders as v1. The demo
reads three full-scale export dates: 2024-08-31 (v1 only), 2024-09-07 (both) and 2024-09-15 (v2 only).

1. A read without mergeSchema takes one file's schema, so the other version's columns are lost.
2. A Delta table created from v1 refuses an append of v2 rows without mergeSchema.
3. A read with mergeSchema keeps both schemas, the Delta append with mergeSchema adds the new
   columns, and one speed column (speed_kmh, or speed_mps x 3.6) leaves no ping without a speed.

DP1 reads the exports with mergeSchema into bronze, and DP2 unifies the speed in silver, as step 3.
The demo table goes to s3a://lakehouse/_benchmarks/schema_evolution/pings.
"""

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from common import lakehouse_session

PINGS = "s3a://app-exports/courier_pings"
DATES = ["2024-08-31", "2024-09-07", "2024-09-15"]
TABLE = "s3a://lakehouse/_benchmarks/schema_evolution/pings"
V2_COLUMNS = ["battery_pct", "gps_accuracy_m", "speed_mps"]


def paths(*dates: str) -> list[str]:
    """Return the export folders of the dates."""
    return [f"{PINGS}/export_date={d}" for d in dates]


def by_version(df):
    """Return the row count per app version, as a dict."""
    return {r.app_version: r.n for r in df.groupBy("app_version").agg(F.count("*").alias("n")).collect()}


def main() -> None:
    """Run the three steps and print what each one shows."""
    spark = lakehouse_session("schema_evolution_demo")

    naive = spark.read.parquet(*paths(*DATES))
    merged = spark.read.option("mergeSchema", "true").parquet(*paths(*DATES))
    lost = sorted(set(merged.columns) - set(naive.columns))
    speed = "speed_kmh" if "speed_kmh" in naive.columns else "speed_mps"
    print(f"[1 naive read] {len(naive.columns)} columns; lost against the merged read: {lost}")
    print(f"[1 naive read] rows per app version: {by_version(naive)}")
    print(f"[1 naive read] pings without {speed}: {naive.where(F.col(speed).isNull()).count():,} of {naive.count():,}")

    v1 = spark.read.parquet(*paths(DATES[0]))
    v2 = spark.read.parquet(*paths(DATES[2]))
    v1.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(TABLE)
    try:
        v2.write.format("delta").mode("append").save(TABLE)
        print("[2 append without mergeSchema] accepted (unexpected)")
    except AnalysisException as error:
        print(f"[2 append without mergeSchema] refused: {str(error).splitlines()[0][:300]}")

    print(f"[3 merged read] {len(merged.columns)} columns; rows per app version: {by_version(merged)}")
    v2.write.format("delta").mode("append").option("mergeSchema", "true").save(TABLE)
    stored = spark.read.format("delta").load(TABLE)
    history = DeltaTable.forPath(spark, TABLE).history().select("version", "operation", "operationParameters").collect()
    print(f"[3 append with mergeSchema] table columns: {stored.columns}")
    for h in sorted(history, key=lambda r: r.version):
        print(f"[3 history] version {h.version}: {h.operation} {dict(h.operationParameters)}")
    unified = merged.withColumn("speed", F.coalesce(F.col("speed_kmh").cast("double"), F.col("speed_mps") * 3.6))
    print(f"[3 unified speed] pings without a speed: {unified.where(F.col('speed').isNull()).count():,} of {merged.count():,}; "
          f"v2 columns present: {[c for c in V2_COLUMNS if c in stored.columns]}")
    spark.stop()


if __name__ == "__main__":
    main()
