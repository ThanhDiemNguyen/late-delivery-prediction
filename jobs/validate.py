"""Validate stage of DP1 to DP3: Great Expectations on Spark DataFrames, one suite per pipeline.

    spark-submit /opt/jobs/validate.py --suite dp1 --ds 2024-08-15            # one business day
    spark-submit /opt/jobs/validate.py --suite dp1 --ds 2024-08-15 --corrupt  # the same, on a corrupted copy

Each check reads only the slice of a table that one run writes: the export date ds of the exports,
the oms rows changed on local day ds, and the silver and gold rows of day ds. So the cost does not
grow with the table. DP1 checks that bronze is what the sources promise (dirty values included);
DP2 checks that silver and gold are clean, consistent and fully joined. The results
go to s3a://lakehouse/_validation/<suite>/ds=<ds>/results.json. With --fail-on-error the job exits 1
when any expectation fails; without it the job exits 0 once the checks ran, so the next task can
publish the results before stopping the pipeline.

GE 0.18 legacy API (SparkDFDataset): every expectation runs as a Spark query on the executors.
"""

import argparse
import json
import sys
from datetime import datetime, timezone

from great_expectations.dataset import SparkDFDataset
from pyspark.sql import DataFrame, SparkSession, functions as F

from common import lakehouse_session

OUT = "s3a://lakehouse/_validation"
LA = "America/Los_Angeles"
LAT, LON = (33.70, 34.82), (-118.95, -117.65)
LEGACY_TRAFFIC = ["Low ", "Medium ", "High ", "Jam ", "NaN "]
LEGACY_AREA = ["Metropolitian ", "Urban ", "Semi-Urban ", "Other"]
PING_COLUMNS = [
    "courier_id", "device_seq", "hub_id", "trip_id", "ping_ts", "lat", "lon", "speed_kmh", "speed_mps",
    "battery_pct", "gps_accuracy_m", "app_version", "uploaded_ts", "export_date", "_ingested_at", "_source",
]


def not_null(*columns):
    """Return one not-null expectation per column."""
    return [("expect_column_values_to_not_be_null", {"column": c}) for c in columns]


def changed_on(column: str):
    """Return a filter for rows whose change column falls on local day ds."""
    return lambda ds: F.to_date(F.from_utc_timestamp(F.col(column), LA)) == F.lit(ds)


def export_date(ds):
    """Return a filter for one export date."""
    return F.col("export_date") == F.lit(ds)


def on_day(column: str):
    """Return a filter for rows whose date column equals ds."""
    return lambda ds: F.col(column) == F.lit(ds)


def date_key(ds):
    """Return a filter for one date_key (YYYYMMDD)."""
    return F.col("date_key") == F.lit(int(ds.replace("-", "")))


def fact_with_dimensions(spark: SparkSession, ds: str) -> DataFrame:
    """Return day ds of fact_delivery left-joined to its four dimensions (NULL key = missing dimension row)."""
    fact = spark.table("gold.fact_delivery").where(date_key(ds))
    dims = {
        "dim_courier": ("courier_sk", "courier_sk"), "dim_hub": ("hub_id", "hub_id"),
        "dim_customer": ("customer_id", "customer_id"), "dim_date": ("date_key", "date_key"),
    }
    for dim, (key, column) in dims.items():
        found = spark.table(f"gold.{dim}").select(F.col(key).alias(f"{dim}_{column}")).distinct()
        fact = fact.join(found, fact[column] == found[f"{dim}_{column}"], "left")
    return fact


def fact_labels(spark: SparkSession, ds: str) -> DataFrame:
    """Return day ds of fact_delivery with the label as 0/1, for its mean."""
    return spark.table("gold.fact_delivery").where(date_key(ds)).select("order_id", F.col("is_late").cast("int").alias("late"))


# Datasets that are built for a check instead of read as one table.
DERIVED = {
    "gold.fact_delivery joined to its dimensions": fact_with_dimensions,
    "gold.fact_delivery labels": fact_labels,
}
CLEAN = {
    "traffic": ["Low", "Medium", "High", "Jam"],
    "weather": ["Sunny", "Cloudy", "Fog", "Windy", "Stormy"],
    "vehicle": ["van", "car", "e-bike"],
    "area": ["Metropolitan", "Urban", "Semi-Urban", "Other"],
}


# suite -> [(table, row filter for day ds or None, [(expectation, kwargs)])]
SUITES = {
    "dp1": [
        ("bronze.raw_hubs", None, [
            ("expect_table_row_count_to_equal", {"value": 40}),
            ("expect_column_values_to_be_unique", {"column": "hub_id"}),
            ("expect_column_values_to_be_between", {"column": "lat", "min_value": LAT[0], "max_value": LAT[1]}),
            ("expect_column_values_to_be_between", {"column": "lon", "min_value": LON[0], "max_value": LON[1]}),
        ]),
        ("bronze.raw_couriers", None, [
            ("expect_column_values_to_be_unique", {"column": "courier_id"}),
            *not_null("home_hub_id", "vehicle_type", "rating", "employment_type", "updated_ts"),
            ("expect_column_values_to_be_in_set", {"column": "vehicle_type", "value_set": ["van", "car", "e-bike"]}),
            ("expect_column_values_to_be_between", {"column": "rating", "min_value": 1, "max_value": 5}),
        ]),
        ("bronze.raw_courier_changes", changed_on("changed_ts"), [
            ("expect_column_values_to_be_unique", {"column": "change_id"}),
            *not_null("courier_id", "changed_ts", "change_reason", "home_hub_id", "vehicle_type", "rating"),
        ]),
        ("bronze.raw_customers", None, [
            ("expect_column_values_to_be_unique", {"column": "customer_id"}),
            *not_null("service_hub_id", "home_lat", "home_lon", "signup_ts"),
            ("expect_column_values_to_be_between", {"column": "home_lat", "min_value": LAT[0], "max_value": LAT[1]}),
        ]),
        ("bronze.raw_orders", changed_on("updated_ts"), [
            ("expect_table_row_count_to_be_between", {"min_value": 1}),
            ("expect_column_values_to_be_unique", {"column": "order_id"}),
            *not_null("order_id", "customer_id", "hub_id", "courier_id", "order_date", "promised_ts", "delivered_ts"),
            # bronze keeps the legacy spellings; a value outside them is a new source problem
            ("expect_column_values_to_be_in_set", {"column": "traffic", "value_set": LEGACY_TRAFFIC}),
            ("expect_column_values_to_be_in_set", {"column": "area", "value_set": LEGACY_AREA}),
            ("expect_column_values_to_be_in_set", {"column": "status", "value_set": ["delivered"]}),
        ]),
        ("bronze.raw_courier_pings", export_date, [
            ("expect_table_row_count_to_be_between", {"min_value": 1}),
            *[("expect_column_to_exist", {"column": c}) for c in PING_COLUMNS],
            *not_null("courier_id", "device_seq", "hub_id", "ping_ts", "lat", "lon", "app_version", "uploaded_ts"),
            ("expect_column_values_to_be_in_set", {"column": "app_version", "value_set": ["3.14.2", "4.0.0"]}),
            # pings stay near LA: a whole-degree margin around the validation box
            ("expect_column_values_to_be_between", {"column": "lat", "min_value": 32.7, "max_value": 35.8}),
            ("expect_column_values_to_be_between", {"column": "lon", "min_value": -119.95, "max_value": -116.65}),
        ]),
        ("bronze.raw_parcel_scans", export_date, [
            ("expect_table_row_count_to_be_between", {"min_value": 1}),
            ("expect_column_values_to_be_unique", {"column": "scan_id"}),
            *not_null("scan_id", "order_id", "hub_id", "scan_type", "scan_ts"),
            ("expect_column_values_to_be_in_set", {"column": "scan_type", "value_set": ["at_hub", "picked_up", "delivered"]}),
        ]),
    ],
    "dp2": [
        ("silver.stg_orders", changed_on("updated_ts"), [
            ("expect_table_row_count_to_be_between", {"min_value": 1}),
            ("expect_column_values_to_be_unique", {"column": "order_id"}),
            *not_null("order_ts", "pickup_ts", "promised_ts", "delivered_ts", "vehicle", "area", "distance_km", "store_lat"),
            # NULL traffic and weather are the seed's null block; any value must be a clean one
            *[("expect_column_values_to_be_in_set", {"column": c, "value_set": v}) for c, v in CLEAN.items()],
            ("expect_column_values_to_be_between", {"column": "store_lat", "min_value": LAT[0], "max_value": LAT[1]}),
            ("expect_column_values_to_be_between", {"column": "store_lon", "min_value": LON[0], "max_value": LON[1]}),
            ("expect_column_pair_values_A_to_be_greater_than_B", {"column_A": "pickup_ts", "column_B": "order_ts"}),
            ("expect_column_pair_values_A_to_be_greater_than_B", {"column_A": "delivered_ts", "column_B": "pickup_ts"}),
            ("expect_column_values_to_be_between", {"column": "distance_km", "min_value": 0, "max_value": 40}),
        ]),
        ("silver.stg_courier_pings", on_day("ping_date"), [
            ("expect_table_row_count_to_be_between", {"min_value": 1}),
            ("expect_compound_columns_to_be_unique", {"column_list": ["courier_id", "device_seq"]}),
            *not_null("courier_id", "device_seq", "ping_ts", "speed_kmh", "schema_version"),
            ("expect_column_values_to_be_in_set", {"column": "schema_version", "value_set": [1, 2]}),
            ("expect_column_values_to_be_between", {"column": "speed_kmh", "min_value": 0, "max_value": 200}),
        ]),
        ("silver.stg_parcel_scans", on_day("scan_date"), [
            ("expect_table_row_count_to_be_between", {"min_value": 1}),
            ("expect_column_values_to_be_unique", {"column": "scan_id"}),
        ]),
        ("gold.dim_courier", None, [
            ("expect_column_values_to_be_unique", {"column": "courier_sk"}),
            *not_null("courier_id", "hub_id", "vehicle_type", "rating", "valid_from_ts", "valid_to_ts", "is_current"),
            ("expect_column_pair_values_A_to_be_greater_than_B", {"column_A": "valid_to_ts", "column_B": "valid_from_ts"}),
        ]),
        ("gold.fact_delivery", date_key, [
            ("expect_table_row_count_to_be_between", {"min_value": 1}),
            ("expect_column_values_to_be_unique", {"column": "order_id"}),
            *not_null("courier_sk", "hub_id", "customer_id", "date_key", "is_late", "delivery_min", "promised_min"),
            ("expect_column_values_to_be_between", {"column": "delivery_min", "min_value": 10, "max_value": 1000}),
            ("expect_column_values_to_be_between", {"column": "promised_min", "min_value": 120, "max_value": 300}),
        ]),
        ("gold.fact_delivery joined to its dimensions", None, [
            *not_null("dim_courier_courier_sk", "dim_hub_hub_id", "dim_customer_customer_id", "dim_date_date_key"),
        ]),
        ("gold.fact_delivery labels", None, [
            ("expect_column_mean_to_be_between", {"column": "late", "min_value": 0.08, "max_value": 0.30}),
        ]),
        ("gold.fact_courier_activity_hourly", on_day("activity_date"), [
            ("expect_table_row_count_to_be_between", {"min_value": 1}),
            ("expect_compound_columns_to_be_unique", {"column_list": ["courier_id", "hour_start"]}),
            ("expect_column_values_to_be_between", {"column": "ping_count", "min_value": 1, "max_value": 144}),
            ("expect_column_values_to_be_between", {"column": "idle_min", "min_value": 0, "max_value": 60}),
            ("expect_column_values_to_be_between", {"column": "km_moved", "min_value": 0, "max_value": 120}),
        ]),
    ],
}


def corrupt(table: str, df: DataFrame) -> DataFrame:
    """Return a corrupted copy of one table's slice, as a broken source batch would look."""
    if table == "bronze.raw_orders":
        return df.unionByName(df.limit(25)).withColumn("traffic", F.when(F.rand(1) < 0.01, F.lit("Gridlock")).otherwise(F.col("traffic")))
    if table == "bronze.raw_courier_pings":
        broken = F.rand(2) < 0.01
        return df.withColumn("courier_id", F.when(broken, F.lit(None)).otherwise(F.col("courier_id"))).withColumn(
            "lat", F.when(F.rand(3) < 0.001, F.lit(0.0)).otherwise(F.col("lat"))
        )
    if table == "bronze.raw_parcel_scans":
        return df.withColumn("scan_type", F.when(F.rand(4) < 0.01, F.lit("lost")).otherwise(F.col("scan_type")))
    return df


def run_suite(spark: SparkSession, suite: str, ds: str, corrupted: bool) -> dict:
    """Run every expectation of the suite on day ds and return the results."""
    results = []
    for table, row_filter, expectations in SUITES[suite]:
        df = DERIVED[table](spark, ds) if table in DERIVED else spark.table(table)
        if row_filter is not None:
            df = df.where(row_filter(ds))
        if corrupted:
            df = corrupt(table, df)
        dataset = SparkDFDataset(df.cache())
        for name, kwargs in expectations:
            outcome = getattr(dataset, name)(**kwargs, result_format="BASIC")
            observed = outcome.result or {}
            results.append({
                "table": table, "expectation": name, "kwargs": {k: v for k, v in kwargs.items() if k != "value_set"},
                "success": bool(outcome.success),
                "observed": {k: observed[k] for k in ("observed_value", "element_count", "unexpected_count", "unexpected_percent") if k in observed},
            })
        df.unpersist()
    failed = [r for r in results if not r["success"]]
    return {
        "suite": suite, "ds": ds, "corrupted": corrupted, "run_at": datetime.now(timezone.utc).isoformat(),
        "evaluated": len(results), "failed": len(failed), "success": not failed, "results": results,
    }


def main() -> None:
    """Run a suite, write its results to the lakehouse and print a summary."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--suite", choices=sorted(SUITES), required=True)
    parser.add_argument("--ds", required=True, help="business day, YYYY-MM-DD (local LA date)")
    parser.add_argument("--corrupt", action="store_true", help="validate a corrupted copy of the slice")
    parser.add_argument("--fail-on-error", action="store_true", help="exit 1 when any expectation fails")
    args = parser.parse_args()
    spark = lakehouse_session(f"validate_{args.suite}")
    report = run_suite(spark, args.suite, args.ds, args.corrupt)
    name = "results-corrupted.json" if args.corrupt else "results.json"
    path = f"{OUT}/{args.suite}/ds={args.ds}/{name}"
    spark.createDataFrame([(json.dumps(report),)], "json string").coalesce(1).write.mode("overwrite").text(path)
    for r in report["results"]:
        if not r["success"]:
            print(f"FAIL {r['table']} {r['expectation']} {r['kwargs']} observed {r['observed']}")
    print(f"suite {args.suite} ds {args.ds}{' (corrupted)' if args.corrupt else ''}: "
          f"{report['evaluated'] - report['failed']} of {report['evaluated']} expectations passed; results in {path}")
    spark.stop()
    sys.exit(1 if args.fail_on_error and report["failed"] else 0)


if __name__ == "__main__":
    main()
