"""Skew benchmark: courier pings LEFT JOIN their trip's order on trip_id, where idle pings have no trip.

    spark-submit /opt/jobs/skew_benchmark.py --stage-trips                 # once: oms orders to Parquet
    spark-submit /opt/jobs/skew_benchmark.py --method baseline [--days N]  # then each method

About 30% of the pings are idle and carry a NULL trip_id. A sort-merge join hash-partitions both
sides by the key, and every NULL lands in the same partition, so one task gets 30% of the rows while
the others share the rest. A NULL never matches, so the join result is known for those rows; the
methods differ in how they keep them from piling up:

    baseline       adaptive query execution (AQE) off: the NULL partition is one straggler task
    aqe            AQE on with its skew-join rule, which splits the oversized partition of the left
                   side of a LEFT OUTER join into tasks of the advisory size
    isolate_nulls  AQE off; the NULL rows skip the join and are unioned back with NULL order columns
    salt_nulls     AQE off; a NULL trip_id is replaced in the join key by a random negative number
                   -1 to -N, which no order id matches, so the NULL rows spread over N keys and
                   partitions; the order side is not copied

Broadcast joins are off in every method: the order side (80.8 million rows) is too large to
broadcast, and the comparison is about the shuffle. Every method computes the same checksum over
the join result, so the methods must agree, and the result goes to
s3a://lakehouse/_benchmarks/skew/<method>.json with the wall time and every stage's task metrics.
"""

import argparse
import json
import os
import time

from pyspark.sql import DataFrame, SparkSession, functions as F

from common import lakehouse_session, stage_metrics

PINGS = "s3a://app-exports/courier_pings"
TRIPS = "s3a://lakehouse/_benchmarks/skew/trips"
OUT = "s3a://lakehouse/_benchmarks/skew"
# Each salt key should hold fewer rows than an ordinary partition, and the keys should outnumber the
# partitions so they spread evenly: 1.44 billion NULL rows / 1,000 = 1.4 million per key, against
# about 17 million rows in an ordinary partition of 200. With 64 salts the full-scale run ran out of
# memory: a partition that drew one or two salt keys held 40 to 60 million rows.
SALTS = 1000
SALTED_PARTITIONS = 400
PERIOD_START = "2024-06-01"
METHODS = ("baseline", "aqe", "isolate_nulls", "salt_nulls")


def stage_trips(spark: SparkSession) -> None:
    """Copy the order columns the join needs from oms to Parquet, so each run reads them the same way."""
    url = f"jdbc:postgresql://{os.environ.get('POSTGRES_HOST', 'postgres')}:5432/oms"
    bounds = spark.read.jdbc(url, "(SELECT min(order_id) lo, max(order_id) hi FROM orders) b", properties=jdbc_properties()).first()
    orders = spark.read.jdbc(
        url, "(SELECT order_id, promised_ts, delivered_ts FROM orders) o", column="order_id",
        lowerBound=bounds.lo, upperBound=bounds.hi + 1, numPartitions=16, properties=jdbc_properties(),
    )
    orders.write.mode("overwrite").parquet(TRIPS)
    print(f"staged {spark.read.parquet(TRIPS).count():,} orders to {TRIPS}")


def jdbc_properties() -> dict:
    """Return the JDBC settings; the credentials come from the environment."""
    return {"user": os.environ["POSTGRES_USER"], "password": os.environ["POSTGRES_PASSWORD"],
            "driver": "org.postgresql.Driver", "fetchsize": "50000"}


def read_pings(spark: SparkSession, days: int | None) -> DataFrame:
    """Return the pings with the columns the join needs, optionally only the first N export dates."""
    # The three columns exist in both app schemas, so no schema merge (which reads every footer) is needed.
    pings = spark.read.parquet(PINGS).select("courier_id", "trip_id", "ping_ts", "export_date")
    if days:
        # A literal bound prunes the export_date folders without reading a file; min(export_date)
        # would scan every file first.
        pings = pings.where(F.col("export_date") < F.date_add(F.lit(PERIOD_START).cast("date"), days))
    return pings.drop("export_date")


def configure(spark: SparkSession, method: str) -> None:
    """Set AQE and the shuffle partitions for the method; broadcast joins stay off for every method."""
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.adaptive.enabled", str(method == "aqe").lower())
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", str(method == "aqe").lower())
    # Salting moves the NULL rows into every partition instead of one, so each of the 200 partitions
    # grows by 40% and its sort ran out of the executor's 2 GB at full scale; it gets twice as many.
    spark.conf.set("spark.sql.shuffle.partitions", str(SALTED_PARTITIONS if method == "salt_nulls" else 200))


def join(pings: DataFrame, trips: DataFrame, method: str) -> DataFrame:
    """Return pings LEFT JOIN trips on trip_id = order_id, done the way the method says."""
    if method == "isolate_nulls":
        matched = pings.where(F.col("trip_id").isNotNull()).join(trips, pings.trip_id == trips.order_id, "left")
        idle = pings.where(F.col("trip_id").isNull()).select(
            *pings.columns, *[F.lit(None).cast(f.dataType).alias(f.name) for f in trips.schema.fields]
        )
        return matched.unionByName(idle)
    if method == "salt_nulls":
        # A literal salt on the order side would be folded out of the join keys by the optimizer, so
        # the NULLs are salted in the key itself: -1 to -SALTS, which no order id matches.
        spread = F.coalesce(F.col("trip_id"), -(F.floor(F.rand(7) * SALTS).cast("long") + 1))
        salted = pings.withColumn("join_key", spread)
        return salted.join(trips, salted.join_key == trips.order_id, "left").drop("join_key")
    return pings.join(trips, pings.trip_id == trips.order_id, "left")


def checksum(result: DataFrame) -> dict:
    """Return counts over the join result that every method must reproduce."""
    late = F.col("delivered_ts") > F.col("promised_ts")
    row = result.agg(
        F.count("*").alias("rows"),
        F.count("order_id").alias("matched"),
        F.sum(F.when(late, 1).otherwise(0)).alias("on_late_trips"),
        F.sum(F.xxhash64("courier_id", "ping_ts", "order_id")).alias("hash"),
    ).first()
    return row.asDict()


def main() -> None:
    """Stage the orders, or run one method and record its time and checksum."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--stage-trips", action="store_true")
    parser.add_argument("--method", choices=METHODS)
    parser.add_argument("--days", type=int, default=None, help="only the first N export dates (for a trial)")
    args = parser.parse_args()
    spark = lakehouse_session(f"skew_{args.method or 'stage_trips'}")
    if args.stage_trips:
        stage_trips(spark)
        return
    configure(spark, args.method)
    started = time.perf_counter()
    result = checksum(join(read_pings(spark, args.days), spark.read.parquet(TRIPS), args.method))
    seconds = round(time.perf_counter() - started, 1)
    report = {"method": args.method, "days": args.days, "seconds": seconds, **result,
              "app_id": spark.sparkContext.applicationId, "stages": stage_metrics(spark)}
    spark.createDataFrame([(json.dumps(report),)], "json string").coalesce(1).write.mode("overwrite").text(
        f"{OUT}/{args.method}{'' if args.days is None else f'-{args.days}d'}.json")
    print(json.dumps({k: v for k, v in report.items() if k != "stages"}))
    for stage in report["stages"]:
        print(json.dumps(stage))
    spark.stop()


if __name__ == "__main__":
    main()
