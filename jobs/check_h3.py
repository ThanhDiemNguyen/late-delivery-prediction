"""Proofs for novel idea 1 (H3 zones with Apache Sedona); exit 1 if either fails.

    spark-submit /opt/jobs/check_h3.py

1. Sedona's H3 cells equal the reference library's: for a sample of drop points, the cell Sedona
   computed in gold (ST_H3CellIDs) equals h3-py's latlng_to_cell at the same resolution.
2. Sedona's spatial test finds exactly the broken store points: in bronze orders, the points that
   ST_Contains says are outside the LA service polygon are exactly the rows the generator broke,
   (0, 0) or a sign-flipped latitude, so the flagged rate equals the injected rate.
"""

import sys

import h3
from pyspark.sql import functions as F

from common import lakehouse_session, with_sedona

SAMPLE = 20_000
RESOLUTION = 7
# The generator's hand-drawn LA service polygon (generator/la_hubs.py SERVICE_POLYGON), as (lat, lon)
# pairs; POLYGON_WKT writes them as lon lat.
SERVICE_POLYGON = [
    (34.82, -118.95), (34.82, -117.65), (33.78, -117.65), (33.75, -118.10), (33.762, -118.19),
    (33.71, -118.29), (33.74, -118.41), (33.80, -118.40), (33.842, -118.394), (33.885, -118.41),
    (33.91, -118.425), (33.955, -118.445), (33.985, -118.473), (34.00, -118.495), (34.03, -118.53),
    (34.033, -118.58), (34.03, -118.677), (33.995, -118.806), (34.04, -118.95),
]
POLYGON_WKT = "POLYGON((" + ", ".join(f"{lon} {lat}" for lat, lon in SERVICE_POLYGON + SERVICE_POLYGON[:1]) + "))"


def main() -> None:
    """Run both proofs and print their numbers."""
    spark = with_sedona(lakehouse_session("check_h3"))

    sample = (
        spark.table("silver.stg_orders").select("order_id", "drop_lat", "drop_lon")
        .join(spark.table("gold.fact_delivery").select("order_id", "drop_zone_id"), "order_id")
        .orderBy(F.xxhash64("order_id")).limit(SAMPLE).collect()
    )
    mismatches = [r for r in sample if h3.str_to_int(h3.latlng_to_cell(r.drop_lat, r.drop_lon, RESOLUTION)) != r.drop_zone_id]
    print(f"[h3] {len(sample):,} drop points: Sedona's cell equals h3-py {h3.__version__}'s for "
          f"{len(sample) - len(mismatches):,}; {len(mismatches)} mismatches")

    orders = spark.table("bronze.raw_orders").select(
        F.expr(f"NOT ST_Contains(ST_GeomFromWKT('{POLYGON_WKT}'), ST_Point(store_lon, store_lat))").alias("flagged"),
        (((F.col("store_lat") == 0) & (F.col("store_lon") == 0)) | (F.col("store_lat") < 0)).alias("injected"),
    )
    counts = orders.groupBy().agg(
        F.count("*").alias("rows"),
        F.sum(F.col("flagged").cast("int")).alias("flagged"),
        F.sum(F.col("injected").cast("int")).alias("injected"),
        F.sum((F.col("flagged") & ~F.col("injected")).cast("int")).alias("false_alarms"),
        F.sum((~F.col("flagged") & F.col("injected")).cast("int")).alias("missed"),
    ).first()
    print(f"[geo] bronze orders: {counts.rows:,}; outside the LA polygon (Sedona) {counts.flagged:,} = "
          f"{100 * counts.flagged / counts.rows:.3f}%; injected (0, 0) or flipped {counts.injected:,} = "
          f"{100 * counts.injected / counts.rows:.3f}%; false alarms {counts.false_alarms}, missed {counts.missed}")

    zones = spark.table("gold.dim_zone").agg(
        F.count("*").alias("zones"), F.round(F.avg("area_km2"), 3).alias("km2"), F.sum("customers").alias("customers")
    ).first()
    print(f"[zones] {zones.zones:,} H3 cells (resolution {RESOLUTION}), mean area {zones.km2} km2, {zones.customers:,} customers")
    top = (
        spark.table("gold.feat_zone_daily").where("feature_date = '2024-11-30' AND deliveries_30d >= 300")
        .join(spark.table("gold.dim_zone").select("zone_id", "h3_index", "main_hub_id"), "zone_id")
        .orderBy(F.col("late_rate_30d").desc()).limit(3).collect()
    )
    for z in top:
        print(f"[zones] highest 30-day late rate on 2024-11-30: {z.h3_index} (hub {z.main_hub_id}) {z.late_rate_30d:.1%} of {z.deliveries_30d} deliveries")
    spark.stop()
    sys.exit(1 if mismatches or counts.false_alarms or counts.missed else 0)


if __name__ == "__main__":
    main()
