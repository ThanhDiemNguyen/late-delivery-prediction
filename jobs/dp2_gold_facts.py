"""DP2, gold facts: fact_delivery (one row per order) and fact_courier_activity_hourly.

    spark-submit /opt/jobs/dp2_gold_facts.py                        # MERGE new and changed orders, add new ping dates
    spark-submit /opt/jobs/dp2_gold_facts.py --rebuild              # rewrite both facts from silver
    spark-submit /opt/jobs/dp2_gold_facts.py --rebuild --until 2024-10-31   # as of a local date (incremental demo)

fact_delivery joins each order to the dim_courier version that was valid when the parcel was
picked up (valid_from_ts <= pickup_ts < valid_to_ts), so a later rating or vehicle change does not
rewrite history. It carries the label: is_late = delivered_ts > promised_ts, and drop_zone_id, the H3 cell
(resolution 7, Apache Sedona) of the drop point, which joins to dim_zone.

fact_courier_activity_hourly sums the silver pings per courier and local hour: ping count, km moved
(between consecutive pings), idle minutes (pings with no trip) and mean speed while on a trip. The
pings are joined to dim_hub for the hub's area type.

Both facts have Delta's Change Data Feed on, so DP3 can read just the rows each run changed
(novel idea 2, docs/novel_cdf_incremental.md). fact_delivery is therefore merged by order_id, and
a matched order is updated only when one of its values really changed.
"""

import argparse

from delta.tables import DeltaTable

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from common import lakehouse_session, with_sedona

GOLD = "s3a://lakehouse/gold"
LA = "America/Los_Angeles"
PING_INTERVAL_MIN = 25 / 60


def save(df: DataFrame, table: str, partition_by: str, dynamic: bool) -> None:
    """Write the Delta table at GOLD/<table> (only df's partitions if dynamic), register it, turn on CDF.

    The Change Data Feed property is set through the table's path, so only the Delta log changes
    and the Hive Metastore entry is never altered.
    """
    path = f"{GOLD}/{table}"
    writer = df.write.format("delta").mode("overwrite").partitionBy(partition_by)
    writer = writer.option("partitionOverwriteMode", "dynamic") if dynamic else writer.option("overwriteSchema", "true")
    writer.save(path)
    df.sparkSession.sql(f"CREATE TABLE IF NOT EXISTS gold.{table} USING DELTA LOCATION '{path}'")
    df.sparkSession.sql(f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")


# A matched order is rewritten only when one of these differs (NULL-safe), so CDF lists real changes.
FACT_COMPARED = ["courier_sk", "order_ts", "pickup_ts", "delivered_ts", "promised_ts", "is_late", "distance_km",
                 "traffic", "weather", "vehicle", "area", "category", "drop_zone_id"]


def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in km between two points given as columns of degrees."""
    dlat, dlon = F.radians(lat2 - lat1), F.radians(lon2 - lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    return 2 * 6371.0 * F.asin(F.sqrt(a))


def build_fact_delivery(spark: SparkSession, until: str | None, rebuild: bool) -> None:
    """Merge (or with rebuild, rewrite) fact_delivery from silver orders and the courier version at pickup."""
    orders = spark.table("silver.stg_orders")
    if until:
        orders = orders.where(F.to_date(F.from_utc_timestamp("delivered_ts", LA)) <= F.lit(until))
    orders = orders.alias("o")
    versions = spark.table("gold.dim_courier").select("courier_sk", "courier_id", "valid_from_ts", "valid_to_ts").alias("v")
    valid_at_pickup = (
        (F.col("o.courier_id") == F.col("v.courier_id"))
        & (F.col("o.pickup_ts") >= F.col("v.valid_from_ts"))
        & (F.col("o.pickup_ts") < F.col("v.valid_to_ts"))
    )
    local_order = F.from_utc_timestamp("o.order_ts", LA)
    fact = orders.join(versions, valid_at_pickup, "left").select(
        "o.order_id", "v.courier_sk", "o.courier_id", "o.hub_id", "o.customer_id",
        F.date_format(local_order, "yyyyMMdd").cast("int").alias("date_key"),
        "o.order_ts", "o.pickup_ts", "o.delivered_ts", "o.promised_ts",
        F.round("o.distance_km", 3).alias("distance_km"),
        F.round((F.unix_timestamp("o.delivered_ts") - F.unix_timestamp("o.order_ts")) / 60, 2).alias("delivery_min"),
        F.round("o.promised_min", 2).alias("promised_min"),
        (F.col("o.delivered_ts") > F.col("o.promised_ts")).alias("is_late"),
        "o.traffic", "o.weather", "o.vehicle", "o.area", "o.category",
        F.hour(local_order).alias("order_hour"),
        F.expr("ST_H3CellIDs(ST_Point(o.drop_lon, o.drop_lat), 7, true)[0]").alias("drop_zone_id"),
        F.date_format(local_order, "yyyy-MM").alias("order_month"),
    )
    if rebuild or not spark.catalog.tableExists("gold.fact_delivery"):
        save(fact, "fact_delivery", "order_month", dynamic=False)
    else:
        changed = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in FACT_COMPARED)
        (
            DeltaTable.forName(spark, "gold.fact_delivery").alias("t")
            .merge(fact.alias("s"), "t.order_id = s.order_id AND t.order_month = s.order_month")
            .whenMatchedUpdateAll(condition=changed)
            .whenNotMatchedInsertAll()
            .execute()
        )
    print(f"gold.fact_delivery: {spark.table('gold.fact_delivery').count():,} rows")


def build_fact_activity(spark: SparkSession, until: str | None, rebuild: bool) -> None:
    """Add the ping dates fact_courier_activity_hourly does not have yet (all of them with rebuild)."""
    table = "gold.fact_courier_activity_hourly"
    pings = spark.table("silver.stg_courier_pings")
    if until:
        pings = pings.where(F.col("ping_date") <= F.lit(until))
    if not rebuild and spark.catalog.tableExists(table):
        done = spark.table(table).agg(F.max("activity_date")).first()[0]
        pings = pings.where(F.col("ping_date") > F.lit(done))
    if pings.isEmpty():
        print(f"{table}: no new ping date")
        return
    hubs = F.broadcast(spark.table("gold.dim_hub").select("hub_id", "area_type"))
    previous = Window.partitionBy("courier_id", "ping_date").orderBy("ping_ts")
    moved = haversine_km(F.lag("lat").over(previous), F.lag("lon").over(previous), F.col("lat"), F.col("lon"))
    local = F.from_utc_timestamp("ping_ts", LA)
    activity = (
        pings.join(hubs, "hub_id", "left")
        .withColumn("km_step", F.coalesce(moved, F.lit(0.0)))
        .withColumn("hour_start", F.to_utc_timestamp(F.date_trunc("hour", local), LA))
        .groupBy("courier_id", "hub_id", "area_type", F.col("ping_date").alias("activity_date"), "hour_start")
        .agg(
            F.count("*").alias("ping_count"),
            F.round(F.sum("km_step"), 3).alias("km_moved"),
            F.round(F.sum(F.when(F.col("trip_id").isNull(), PING_INTERVAL_MIN).otherwise(0.0)), 2).alias("idle_min"),
            F.round(F.avg(F.when(F.col("trip_id").isNotNull(), F.col("speed_kmh"))), 2).alias("avg_speed_kmh"),
            F.countDistinct("trip_id").alias("trips"),
        )
        .withColumn("date_key", F.date_format("activity_date", "yyyyMMdd").cast("int"))
    )
    save(activity, "fact_courier_activity_hourly", "activity_date", dynamic=not rebuild)
    print(f"{table}: {spark.table(table).count():,} rows")


def main() -> None:
    """Build the gold facts."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--rebuild", action="store_true", help="rewrite both facts from silver")
    parser.add_argument("--until", help="only orders delivered and pings sent up to this local date")
    args = parser.parse_args()
    spark = with_sedona(lakehouse_session("dp2_gold_facts"))
    build_fact_delivery(spark, args.until, args.rebuild)
    build_fact_activity(spark, args.until, args.rebuild)
    spark.stop()


if __name__ == "__main__":
    main()
