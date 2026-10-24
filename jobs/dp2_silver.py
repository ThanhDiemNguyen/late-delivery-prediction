"""DP2, first half: clean bronze into silver stg_* tables (same grain, one row per real thing).

    spark-submit /opt/jobs/dp2_silver.py              # everything bronze holds that silver does not
    spark-submit /opt/jobs/dp2_silver.py --rebuild    # reprocess every export date and key

What silver fixes:
    orders   legacy spellings trimmed, "NaN" literals to NULL, "Metropolitian" corrected; broken store
             points replaced by the hub's point; order_ts rebuilt from local text (or from the promise
             rule when the text is NaN), with the repeated hour of 2024-11-03 resolved; pickup_ts
             rebuilt with the after-midnight rollover; the latest version of each order kept
    pings    re-uploaded duplicates removed (earliest upload of each courier_id + device_seq kept);
             app v1 and v2 unified into one schema (speed_kmh from speed_kmh or speed_mps)
    others   latest version of each key; scans deduplicated by scan_id

oms tables are merged by key from the bronze rows ingested since the last run; export tables
overwrite only the dates they process (dynamic partition overwrite), so reruns change nothing.
"""

import argparse

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from common import LAKEHOUSE, lakehouse_session

SILVER = f"{LAKEHOUSE}/silver"
LA = "America/Los_Angeles"
LAT, LON = (33.70, 34.82), (-118.95, -117.65)
# The order system's promise rule (Operations): area base minutes + 3 minutes per km hub to drop.
SLA_BASE_MIN = {"Metropolitan": 135, "Urban": 125, "Semi-Urban": 215, "Other": 120}
SLA_MIN_PER_KM = 3.0
# oms table -> (key, change column)
OMS = {
    "hubs": ("hub_id", "updated_ts"),
    "customers": ("customer_id", "updated_ts"),
    "courier_changes": ("change_id", "changed_ts"),
    "orders": ("order_id", "updated_ts"),
}


def latest(df: DataFrame, key: str, changed: str) -> DataFrame:
    """Keep the newest version of each key (bronze appends every version it pulled)."""
    window = Window.partitionBy(key).orderBy(F.col(changed).desc(), F.col("_ingested_at").desc())
    return df.withColumn("_rank", F.row_number().over(window)).where("_rank = 1").drop("_rank")


def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in km between two points given as columns of degrees."""
    dlat, dlon = F.radians(lat2 - lat1), F.radians(lon2 - lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    return 2 * 6371.0 * F.asin(F.sqrt(a))


def seconds_of_day(clock):
    """Seconds since midnight of an HH:MM:SS text column."""
    parts = F.split(clock, ":")
    return parts[0].cast("int") * 3600 + parts[1].cast("int") * 60 + parts[2].cast("int")


def clean_orders(orders: DataFrame, hubs: DataFrame) -> DataFrame:
    """Return orders with clean values, repaired store points and rebuilt order and pickup instants."""
    hub = F.broadcast(hubs.select("hub_id", F.col("lat").alias("hub_lat"), F.col("lon").alias("hub_lon")))
    o = orders.join(hub, "hub_id", "left")
    traffic = F.trim("traffic")
    area = F.regexp_replace(F.trim("area"), "^Metropolitian$", "Metropolitan")
    store_ok = F.col("store_lat").between(*LAT) & F.col("store_lon").between(*LON)
    o = o.select(
        "*",
        F.when(traffic != "NaN", traffic).alias("traffic_clean"),
        F.when(F.col("weather") != "NaN", F.col("weather")).alias("weather_clean"),
        F.trim("vehicle").alias("vehicle_clean"),
        area.alias("area_clean"),
        (~store_ok).alias("store_point_repaired"),
        F.when(store_ok, F.col("store_lat")).otherwise(F.col("hub_lat")).alias("store_lat_clean"),
        F.when(store_ok, F.col("store_lon")).otherwise(F.col("hub_lon")).alias("store_lon_clean"),
    )
    o = o.withColumn("distance_km", haversine_km(F.col("store_lat_clean"), F.col("store_lon_clean"), F.col("drop_lat"), F.col("drop_lon")))
    base = F.coalesce(*[F.when(F.col("area_clean") == a, F.lit(m)) for a, m in SLA_BASE_MIN.items()])
    o = o.withColumn("promised_min", base + SLA_MIN_PER_KM * F.col("distance_km"))

    # order_ts: local text -> UTC. Spark reads an ambiguous local time (the repeated 01:xx hour of
    # 2024-11-03) as the first, daylight-saving occurrence; the promise rule tells the two apart.
    parsed = F.to_utc_timestamp(F.to_timestamp(F.concat_ws(" ", F.col("order_date").cast("string"), F.col("order_time"))), LA)
    derived = F.timestamp_seconds(F.unix_timestamp("promised_ts") - F.round(F.col("promised_min") * 60).cast("long"))
    gap = F.abs(F.unix_timestamp(parsed) - F.unix_timestamp(derived))
    o = o.withColumn(
        "order_ts_source",
        F.when(parsed.isNull(), "promise_rule").when((gap > 1800) & (F.abs(gap - 3600) <= 60), "standard_time").otherwise("order_time"),
    ).withColumn(
        "order_ts",
        F.when(F.col("order_ts_source") == "promise_rule", derived)
        .when(F.col("order_ts_source") == "standard_time", parsed + F.expr("INTERVAL 1 HOUR"))
        .otherwise(parsed),
    )
    # pickup_ts: the local clock difference modulo 24 h (picked up after midnight). Across the end
    # of daylight saving time the clock goes back an hour, so a difference over 12 h is 23 h too long.
    order_clock = F.date_format(F.from_utc_timestamp("order_ts", LA), "HH:mm:ss")
    delay = F.pmod(seconds_of_day(F.col("pickup_time")) - seconds_of_day(order_clock), 86400)
    delay = F.when(delay > 43200, delay - 82800).otherwise(delay)
    o = o.withColumn("pickup_rolled_over", F.col("pickup_time") < order_clock).withColumn(
        "pickup_ts", F.timestamp_seconds(F.unix_timestamp("order_ts") + delay)
    )
    return o.select(
        "order_id", "customer_id", "hub_id", "courier_id", "category",
        "order_ts", "order_ts_source", "pickup_ts", "pickup_rolled_over", "promised_ts", "delivered_ts", "status",
        F.col("weather_clean").alias("weather"), F.col("traffic_clean").alias("traffic"),
        F.col("vehicle_clean").alias("vehicle"), F.col("area_clean").alias("area"),
        F.col("store_lat_clean").alias("store_lat"), F.col("store_lon_clean").alias("store_lon"),
        "store_point_repaired", "drop_lat", "drop_lon", "distance_km", "promised_min",
        "updated_ts", "_ingested_at",
    )


def save(df: DataFrame, table: str, partition_by: str | None = None) -> None:
    """Overwrite the table at SILVER/<table> (only the partitions df holds, if partitioned) and register it."""
    writer = df.write.format("delta").mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(partition_by).option("partitionOverwriteMode", "dynamic")
    else:
        writer = writer.option("overwriteSchema", "true")
    writer.save(f"{SILVER}/{table}")
    df.sparkSession.sql(f"CREATE TABLE IF NOT EXISTS silver.{table} USING DELTA LOCATION '{SILVER}/{table}'")


def merge(spark: SparkSession, df: DataFrame, table: str, key: str, changed: str) -> None:
    """Upsert df into silver.<table> by key, keeping the newer version; create the table on first run."""
    if not spark.catalog.tableExists(f"silver.{table}"):
        save(df, table)
        return
    (
        DeltaTable.forName(spark, f"silver.{table}").alias("t")
        .merge(df.alias("s"), f"t.{key} = s.{key}")
        .whenMatchedUpdateAll(condition=f"s.{changed} >= t.{changed}")
        .whenNotMatchedInsertAll()
        .execute()
    )


def silver_oms(spark: SparkSession, rebuild: bool) -> None:
    """Merge the bronze oms rows ingested since the last run into silver, cleaning orders."""
    for table, (key, changed) in OMS.items():
        silver = f"silver.stg_{table}"
        bronze = spark.table(f"bronze.raw_{table}")
        if not rebuild and spark.catalog.tableExists(silver):
            since = spark.table(silver).agg(F.max("_ingested_at")).first()[0]
            bronze = bronze.where(F.col("_ingested_at") > F.lit(since))
        rows = latest(bronze, key, changed).drop("_source")
        if table == "orders":
            rows = clean_orders(rows, spark.table("silver.stg_hubs"))
        if rebuild:
            save(rows, f"stg_{table}")
        else:
            merge(spark, rows, f"stg_{table}", key, changed)
        print(f"{silver}: {spark.table(silver).count():,} rows")


def new_dates(spark: SparkSession, bronze: str, silver: str, column: str, rebuild: bool) -> list:
    """Return the bronze export dates silver has not processed yet (all of them with rebuild)."""
    done = None if rebuild or not spark.catalog.tableExists(silver) else spark.table(silver).agg(F.max(column)).first()[0]
    dates = spark.table(bronze).select("export_date").distinct()
    if done is not None:
        dates = dates.where(F.col("export_date") > F.lit(done))
    return sorted(r.export_date for r in dates.collect())


def silver_pings(spark: SparkSession, rebuild: bool) -> None:
    """Deduplicate and unify the new export dates of pings into silver.stg_courier_pings."""
    dates = new_dates(spark, "bronze.raw_courier_pings", "silver.stg_courier_pings", "ping_date", rebuild)
    if not dates:
        print("silver.stg_courier_pings: no new export date")
        return
    raw = spark.table("bronze.raw_courier_pings").where(F.col("export_date").isin(dates))
    first_upload = Window.partitionBy("courier_id", "device_seq").orderBy("uploaded_ts")
    pings = (
        raw.withColumn("_rank", F.row_number().over(first_upload)).where("_rank = 1")
        .select(
            "courier_id", "device_seq", "hub_id", "trip_id", "ping_ts", "lat", "lon",
            F.coalesce(F.col("speed_kmh").cast("double"), F.round(F.col("speed_mps") * 3.6, 2)).alias("speed_kmh"),
            "battery_pct", "gps_accuracy_m", "app_version",
            F.when(F.col("app_version") == "4.0.0", 2).otherwise(1).alias("schema_version"),
            "uploaded_ts",
            F.to_date(F.from_utc_timestamp("ping_ts", LA)).alias("ping_date"),
        )
    )
    save(pings, "stg_courier_pings", partition_by="ping_date")
    # The window's shuffle leaves about 50 small files in each date. Compact the new dates into
    # files ordered by courier, so a courier's day sits in one file (docs/storage_optimization.md).
    listed = ", ".join(f"DATE'{d}'" for d in dates)
    spark.sql(f"OPTIMIZE silver.stg_courier_pings WHERE ping_date IN ({listed}) ZORDER BY (courier_id)")
    print(f"silver.stg_courier_pings: {len(dates)} dates processed ({dates[0]} to {dates[-1]}), compacted")


def silver_scans(spark: SparkSession, rebuild: bool) -> None:
    """Deduplicate the new export dates of scans by scan_id into silver.stg_parcel_scans."""
    dates = new_dates(spark, "bronze.raw_parcel_scans", "silver.stg_parcel_scans", "scan_date", rebuild)
    if not dates:
        print("silver.stg_parcel_scans: no new export date")
        return
    raw = spark.table("bronze.raw_parcel_scans").where(F.col("export_date").isin(dates))
    first_upload = Window.partitionBy("scan_id").orderBy("uploaded_ts")
    scans = (
        raw.withColumn("_rank", F.row_number().over(first_upload)).where("_rank = 1")
        .select("scan_id", "order_id", "courier_id", "hub_id", "scan_type", "scan_ts", "uploaded_ts",
                F.to_date(F.from_utc_timestamp("scan_ts", LA)).alias("scan_date"))
    )
    save(scans, "stg_parcel_scans", partition_by="scan_date")
    print(f"silver.stg_parcel_scans: {len(dates)} dates processed ({dates[0]} to {dates[-1]})")


def main() -> None:
    """Build the silver tables."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--rebuild", action="store_true", help="reprocess everything bronze holds")
    args = parser.parse_args()
    spark = lakehouse_session("dp2_silver")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS silver LOCATION '{SILVER}'")
    silver_oms(spark, args.rebuild)
    silver_pings(spark, args.rebuild)
    silver_scans(spark, args.rebuild)
    spark.stop()


if __name__ == "__main__":
    main()
