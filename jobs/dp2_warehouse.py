"""DP2, warehouse: copy gold into the Postgres star schema in dwh, with declared keys.

    spark-submit /opt/jobs/dp2_warehouse.py

1. Apply jobs/sql/dwh_schema.sql (CREATE TABLE IF NOT EXISTS, with primary and foreign keys).
2. Write each table's rows to dwh.staging.<table> with Spark JDBC.
3. In one transaction: empty the star schema and fill it from staging, dimensions before facts,
   so the foreign keys are checked on every row; then drop staging. A failure rolls everything back,
   and BI tools never see a half-loaded warehouse.

fact_delivery keeps the last 90 days of the period and obt_delivery the last 30.
"""

import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

from common import lakehouse_session

DWH_URL = os.environ.get("DWH_JDBC_URL", "jdbc:postgresql://postgres:5432/dwh")
SCHEMA_SQL = Path(__file__).resolve().parent / "sql" / "dwh_schema.sql"
FACT_DAYS, OBT_DAYS = 90, 30
LOAD_ORDER = ["dim_date", "dim_hub", "dim_zone", "dim_customer", "dim_courier", "fact_delivery", "obt_delivery"]


def _credentials() -> dict:
    """Return the JDBC user, password and driver from the environment."""
    return {"user": os.environ["POSTGRES_USER"], "password": os.environ["POSTGRES_PASSWORD"], "driver": "org.postgresql.Driver"}


def execute(spark: SparkSession, statements: list[str]) -> None:
    """Run SQL statements on dwh in one transaction through the JVM's JDBC driver."""
    jvm = spark._jvm
    props = jvm.java.util.Properties()
    for key, value in _credentials().items():
        props.setProperty(key, value)
    conn = jvm.java.sql.DriverManager.getConnection(DWH_URL, props)
    try:
        conn.setAutoCommit(False)
        statement = conn.createStatement()
        for sql in statements:
            statement.execute(sql)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def tables(spark: SparkSession) -> dict[str, DataFrame]:
    """Return the rows of every warehouse table, built from gold."""
    fact = spark.table("gold.fact_delivery")
    last = fact.agg(F.max(F.to_date(F.from_utc_timestamp("order_ts", "America/Los_Angeles")))).first()[0]
    order_date = F.to_date(F.from_utc_timestamp("order_ts", "America/Los_Angeles"))
    fact_columns = ["order_id", "courier_sk", "hub_id", "customer_id", "date_key", "order_ts", "pickup_ts", "delivered_ts",
                    "promised_ts", "distance_km", "delivery_min", "promised_min", "is_late", "traffic", "weather",
                    "vehicle", "area", "category", "order_hour", "drop_zone_id"]
    recent = fact.where(order_date > F.date_sub(F.lit(last), FACT_DAYS)).select(*fact_columns)
    hub = spark.table("gold.dim_hub")
    courier = spark.table("gold.dim_courier")
    customer = spark.table("gold.dim_customer")
    date = spark.table("gold.dim_date")
    obt = (
        fact.where(order_date > F.date_sub(F.lit(last), OBT_DAYS)).alias("f")
        .join(date.alias("d"), "date_key").join(hub.alias("h"), "hub_id").join(courier.alias("c"), "courier_sk")
        .join(customer.alias("u"), "customer_id")
        .select(
            "f.order_id", F.col("d.date").alias("order_date"), "f.order_hour", "d.day_of_week", "d.is_weekend",
            "d.is_peak_week", "d.is_holiday", "f.hub_id", F.col("h.name").alias("hub_name"),
            F.col("h.area_type").alias("hub_area_type"), F.col("h.capacity_tier").alias("hub_capacity_tier"),
            "f.courier_id", F.col("c.vehicle_type").alias("courier_vehicle_type"), F.col("c.rating").alias("courier_rating"),
            F.col("c.employment_type").alias("courier_employment"), "f.customer_id", F.col("u.segment").alias("customer_segment"),
            "f.traffic", "f.weather", "f.category", "f.distance_km", "f.delivery_min", "f.promised_min", "f.is_late",
            "f.order_ts", "f.pickup_ts", "f.delivered_ts", "f.promised_ts",
        )
    )
    return {
        "dim_date": date,
        "dim_hub": hub,
        "dim_zone": spark.table("gold.dim_zone"),
        "dim_customer": customer,
        "dim_courier": courier.select("courier_sk", "courier_id", "hub_id", "vehicle_type", "rating", "employment_type",
                                      "valid_from_ts", "valid_to_ts", "is_current"),
        "fact_delivery": recent,
        "obt_delivery": obt,
    }


# Tables written in this order, so a BRIN index on the column skips whole block ranges.
PHYSICAL_ORDER = {"obt_delivery": "order_ts"}


def main() -> None:
    """Load gold into the warehouse."""
    spark = lakehouse_session("dp2_warehouse")
    execute(spark, [SCHEMA_SQL.read_text(), "CREATE SCHEMA IF NOT EXISTS staging"])
    rows = tables(spark)
    for name in LOAD_ORDER:
        rows[name].repartition(4).write.jdbc(DWH_URL, f"staging.{name}", mode="overwrite", properties={**_credentials(), "batchsize": "10000"})
    swap = [f"TRUNCATE {', '.join(reversed(LOAD_ORDER))}"]
    for name in LOAD_ORDER:
        columns = ", ".join(rows[name].columns)
        order = f" ORDER BY {PHYSICAL_ORDER[name]}" if name in PHYSICAL_ORDER else ""
        swap.append(f"INSERT INTO public.{name} ({columns}) SELECT {columns} FROM staging.{name}{order}")
    swap.append("DROP SCHEMA staging CASCADE")
    execute(spark, swap)
    counts = spark.read.jdbc(
        DWH_URL,
        "(" + " UNION ALL ".join(f"SELECT '{n}' AS t, count(*) AS n FROM public.{n}" for n in LOAD_ORDER) + ") AS c",
        properties=_credentials(),
    ).collect()
    for row in counts:
        print(f"dwh.{row.t}: {row.n:,} rows")
    spark.stop()


if __name__ == "__main__":
    main()
