"""DP2, gold dimensions: dim_courier (SCD type 2), dim_hub, dim_customer and dim_date.

    spark-submit /opt/jobs/dp2_gold_dims.py                       # add what silver has that gold does not
    spark-submit /opt/jobs/dp2_gold_dims.py --until 2024-08-31    # courier changes up to a local date only
    spark-submit /opt/jobs/dp2_gold_dims.py --rebuild             # rebuild every dimension from silver

dim_courier keeps one row per version of a courier: courier_sk, courier_id, hub_id, vehicle_type,
rating, employment_type, valid_from_ts (inclusive), valid_to_ts (exclusive, 9999-12-31 while
open) and is_current. A version starts only when a tracked attribute really changes: a hash of the
attributes is compared with the previous row, so rating reviews that change nothing add no
version. New changes are applied with one Delta MERGE that closes each courier's open version and
inserts the new ones. courier_sk is the change_id that opened the version, so it is stable.
"""

import argparse
from datetime import date, timedelta

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from common import lakehouse_session

GOLD = "s3a://lakehouse/gold"
LA = "America/Los_Angeles"
TRACKED = ["home_hub_id", "vehicle_type", "rating", "employment_type"]
OPEN_END = "9999-12-31 00:00:00"
PERIOD = (date(2024, 6, 1), date(2024, 12, 1))
PEAK_WEEKS = [(date(2024, 7, 15), date(2024, 7, 21), "July sale"), (date(2024, 11, 25), date(2024, 11, 30), "Black Friday")]
US_HOLIDAYS = {date(2024, 7, 4): "Independence Day", date(2024, 9, 2): "Labor Day", date(2024, 11, 11): "Veterans Day",
               date(2024, 11, 28): "Thanksgiving"}


def save(df: DataFrame, table: str) -> None:
    """Overwrite the Delta table at GOLD/<table> and register it as gold.<table>."""
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD}/{table}")
    df.sparkSession.sql(f"CREATE TABLE IF NOT EXISTS gold.{table} USING DELTA LOCATION '{GOLD}/{table}'")


def courier_versions(changes: DataFrame, current: DataFrame | None) -> DataFrame:
    """Return the new versions that the changes open, after each courier's current version.

    current holds the open dim_courier rows (or None on the first build). It is put in front of
    each courier's new changes so the first new change is compared with it.
    """
    attrs = F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in TRACKED]), 256)
    new = changes.select("courier_id", F.col("change_id").alias("courier_sk"), F.col("changed_ts").alias("valid_from_ts"), *TRACKED)
    new = new.withColumn("_attrs", attrs).withColumn("_is_new", F.lit(True))
    if current is not None:
        prior = current.select("courier_id", "courier_sk", "valid_from_ts", F.col("hub_id").alias("home_hub_id"), *TRACKED[1:])
        new = prior.withColumn("_attrs", attrs).withColumn("_is_new", F.lit(False)).unionByName(new)
    by_time = Window.partitionBy("courier_id").orderBy("valid_from_ts", "courier_sk")
    starts = new.withColumn("_previous", F.lag("_attrs").over(by_time)).where(
        F.col("_previous").isNull() | (F.col("_attrs") != F.col("_previous"))
    )
    versions = starts.where("_is_new").withColumn("valid_to_ts", F.lead("valid_from_ts").over(by_time))
    return versions.select(
        "courier_sk", "courier_id", F.col("home_hub_id").alias("hub_id"), *TRACKED[1:], "valid_from_ts",
        F.coalesce("valid_to_ts", F.to_timestamp(F.lit(OPEN_END))).alias("valid_to_ts"),
        F.col("valid_to_ts").isNull().alias("is_current"),
    )


def build_dim_courier(spark: SparkSession, until: str | None, rebuild: bool) -> None:
    """Create dim_courier, or MERGE the courier changes it has not seen yet into it.

    The changes after the newest version's opening change_id are compared with each courier's open
    version. No-op reviews after that id are compared again on the next run and open nothing, so
    reruns change nothing.
    """
    changes = spark.table("silver.stg_courier_changes")
    if until:
        changes = changes.where(F.to_date(F.from_utc_timestamp("changed_ts", LA)) <= F.lit(until))
    if rebuild or not spark.catalog.tableExists("gold.dim_courier"):
        save(courier_versions(changes, None), "dim_courier")
    else:
        seen = spark.table("gold.dim_courier").agg(F.max("courier_sk")).first()[0]
        current = spark.table("gold.dim_courier").where("is_current")
        versions = courier_versions(changes.where(F.col("change_id") > F.lit(seen)), current).cache()
        if versions.isEmpty():
            print(f"gold.dim_courier: no new version after change {seen}")
            return
        # Rows with a merge key close the matching open version; rows without one are inserted.
        closes = versions.groupBy("courier_id").agg(F.min("valid_from_ts").alias("close_at"))
        closes = closes.select(F.col("courier_id").alias("_merge_key"), "close_at")
        inserts = versions.withColumn("_merge_key", F.lit(None).cast("int"))
        columns = ["courier_sk", "courier_id", "hub_id", *TRACKED[1:], "valid_from_ts", "valid_to_ts", "is_current"]
        (
            DeltaTable.forName(spark, "gold.dim_courier").alias("t")
            .merge(inserts.unionByName(closes, allowMissingColumns=True).alias("s"), "t.courier_id = s._merge_key AND t.is_current")
            .whenMatchedUpdate(set={"valid_to_ts": "s.close_at", "is_current": "false"})
            .whenNotMatchedInsert(condition="s._merge_key IS NULL", values={c: f"s.{c}" for c in columns})
            .execute()
        )
        versions.unpersist()
    dim = spark.table("gold.dim_courier")
    print(f"gold.dim_courier: {dim.count():,} versions, {dim.where('is_current').count():,} current")


def build_small_dims(spark: SparkSession) -> None:
    """Rebuild dim_hub, dim_customer and dim_date (type 1: the current values)."""
    hubs = spark.table("silver.stg_hubs").select("hub_id", "name", "city", "area_type", "lat", "lon", "capacity_tier")
    save(hubs, "dim_hub")
    customers = spark.table("silver.stg_customers").select(
        "customer_id", F.col("service_hub_id").alias("hub_id"), "home_lat", "home_lon", "segment", "signup_ts"
    )
    save(customers, "dim_customer")
    days = [PERIOD[0] + timedelta(n) for n in range((PERIOD[1] - PERIOD[0]).days + 1)]
    rows = []
    for d in days:
        peak = next((name for first, last, name in PEAK_WEEKS if first <= d <= last), None)
        rows.append((int(d.strftime("%Y%m%d")), d, d.isoweekday(), d.isoweekday() >= 6, d.isocalendar()[1], d.month,
                     peak is not None, peak, d in US_HOLIDAYS, US_HOLIDAYS.get(d)))
    schema = ("date_key int, date date, day_of_week int, is_weekend boolean, iso_week int, month int, "
              "is_peak_week boolean, peak_name string, is_holiday boolean, holiday_name string")
    save(spark.createDataFrame(rows, schema), "dim_date")
    for t in ("dim_hub", "dim_customer", "dim_date"):
        print(f"gold.{t}: {spark.table(f'gold.{t}').count():,} rows")


def main() -> None:
    """Build the gold dimensions."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--until", help="apply courier changes up to this local date only")
    parser.add_argument("--rebuild", action="store_true", help="rebuild dim_courier from all of silver")
    parser.add_argument("--part", choices=["courier", "small"], help="one part only")
    args = parser.parse_args()
    spark = lakehouse_session("dp2_gold_dims")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS gold LOCATION '{GOLD}'")
    if args.part in (None, "small"):
        build_small_dims(spark)
    if args.part in (None, "courier"):
        build_dim_courier(spark, args.until, args.rebuild)
    spark.stop()


if __name__ == "__main__":
    main()
