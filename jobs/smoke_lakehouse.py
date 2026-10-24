"""Smoke test of the lakehouse: Spark reads both source systems and writes a Delta table to the catalog.

    docker compose exec spark-master /opt/spark/bin/spark-submit /opt/jobs/smoke_lakehouse.py

1. Reads one day of v1 pings from s3a://app-exports (S3A on SeaweedFS).
2. Reads the order count from Postgres oms (JDBC).
3. Writes pings per hub as the Delta table smoke.pings_per_hub in s3a://lakehouse, registered in
   the Hive Metastore, then reads it back by name.
"""

import os

from pyspark.sql import functions as F

from common import lakehouse_session

DAY = "2024-06-01"


def main() -> None:
    """Run the three checks and print what each one found."""
    spark = lakehouse_session("smoke_lakehouse")

    pings = spark.read.parquet(f"s3a://app-exports/courier_pings/export_date={DAY}/")
    print(f"[s3a] pings on {DAY}: {pings.count():,} rows, columns {pings.columns}")

    orders = spark.read.jdbc(
        "jdbc:postgresql://postgres:5432/oms",
        "(SELECT count(*) AS n FROM orders) AS t",
        properties={"user": os.environ["POSTGRES_USER"], "password": os.environ["POSTGRES_PASSWORD"], "driver": "org.postgresql.Driver"},
    )
    print(f"[jdbc] oms.orders: {orders.first().n:,} rows")

    spark.sql("CREATE DATABASE IF NOT EXISTS smoke LOCATION 's3a://lakehouse/smoke'")
    per_hub = pings.groupBy("hub_id").agg(F.count("*").alias("pings"), F.countDistinct("courier_id").alias("couriers"))
    # Dropped first: overwriting an existing managed Delta table makes Spark try to alter the
    # metastore schema, which Hive Metastore 3.1 rejects with a logged (harmless) error.
    spark.sql("DROP TABLE IF EXISTS smoke.pings_per_hub")
    per_hub.write.format("delta").saveAsTable("smoke.pings_per_hub")
    back = spark.table("smoke.pings_per_hub")
    detail = spark.sql("DESCRIBE DETAIL smoke.pings_per_hub").select("format", "location", "numFiles").first()
    print(f"[delta] smoke.pings_per_hub: {back.count()} hubs, {back.agg(F.sum('pings')).first()[0]:,} pings; "
          f"format {detail.format}, {detail.numFiles} files at {detail.location}")
    print(f"[catalog] tables in smoke: {[t.name for t in spark.catalog.listTables('smoke')]}")
    spark.stop()


if __name__ == "__main__":
    main()
