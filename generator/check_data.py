"""Check every injected rate in the generated source data and print its characteristics.

    uv run python -m generator.check_data --scale 0.01

Reads oms with SQL and the app-exports bucket with pyarrow. Each check compares a measured value
with a target and a tolerance; the command exits with status 1 if any check fails. The printed
tables are the numbers in docs/data_characteristics.md. At full scale the same checks run in Spark.
"""

from __future__ import annotations

import argparse
import io
import sys
from collections import defaultdict
from dataclasses import dataclass

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from generator import db
from generator.app_exports import BUCKET, DUPLICATE_SHARE, IDLE_SHARE, PINGS, PINGS_PER_SHIFT, SCANS
from generator.la_config import AREA_MIX, FULL_COURIERS, FULL_CUSTOMERS, TIME_ZONE
from generator.oms_orders import LEGACY_AREA, local_days, orders_per_day, seed_patterns
from generator.storage import s3_client

CLEAN_AREA = "replace(trim(area), 'Metropolitian', 'Metropolitan')"


@dataclass
class Check:
    """One measured value against its target: passes when |measured - target| <= tolerance."""

    name: str
    measured: float
    target: float
    tolerance: float

    @property
    def passed(self) -> bool:
        """Whether the measured value is within the tolerance."""
        return abs(self.measured - self.target) <= self.tolerance


def _one(cur, sql: str, params=None):
    """Return the single value of a one-row, one-column query."""
    cur.execute(sql, params)
    return cur.fetchone()[0]


def check_oms(cur, scale: float, checks: list[Check], facts: dict) -> None:
    """Check the order system tables and record their characteristics."""
    patterns = seed_patterns()
    counts = {t: _one(cur, f"SELECT count(*) FROM {t}") for t in ("hubs", "couriers", "courier_changes", "customers", "orders")}
    checks += [
        Check("hubs", counts["hubs"], 40, 0),
        Check("couriers", counts["couriers"], round(FULL_COURIERS * scale), 0),
        Check("customers", counts["customers"], round(FULL_CUSTOMERS * scale), 0),
        Check("orders", counts["orders"], int(orders_per_day(scale).sum()), 0),
        Check("audit rows per courier", counts["courier_changes"] / counts["couriers"], 5.75, 0.75),
    ]
    pct = lambda sql: 100 * float(_one(cur, sql))  # noqa: E731
    checks.append(Check("late share %", pct("SELECT avg((delivered_ts > promised_ts)::int) FROM orders"), 17.0, 1.5))
    cur.execute("SELECT trim(traffic), avg((delivered_ts > promised_ts)::int) FROM orders WHERE traffic <> 'NaN ' GROUP BY 1")
    by_traffic = {k: 100 * float(v) for k, v in cur.fetchall()}
    ordered = by_traffic["Low"] < by_traffic["Medium"] < by_traffic["High"] < by_traffic["Jam"]
    checks.append(Check("late share rises Low < Medium < High < Jam (1 = yes)", float(ordered), 1, 0))
    facts["late share by traffic %"] = {k: round(by_traffic[k], 1) for k in ("Low", "Medium", "High", "Jam")}

    cur.execute("SELECT hub_id, count(*) FROM orders GROUP BY 1 ORDER BY 2 DESC")
    hub_counts = pd.Series(dict(cur.fetchall()), dtype=float) / counts["orders"] * 100
    checks += [
        Check("orders at Downtown LA %", hub_counts.get(1, 0), 25.0, 1.0),
        Check("orders at the top 10 hubs %", hub_counts.head(10).sum(), 60.0, 1.5),
    ]
    cur.execute(f"SELECT {CLEAN_AREA}, count(*) FROM orders GROUP BY 1")
    areas = {k: 100 * v / counts["orders"] for k, v in cur.fetchall()}
    checks += [Check(f"orders in {a} %", areas.get(a, 0), 100 * s, 1.0) for a, s in AREA_MIX.items()]
    cur.execute("SELECT DISTINCT area FROM orders")
    checks.append(Check("area spellings are the legacy ones (1 = yes)", float({r[0] for r in cur.fetchall()} == set(LEGACY_AREA.values())), 1, 0))

    checks += [
        Check("null block (NaN traffic, weather, order_time) %", pct("SELECT avg((order_time = 'NaN ' AND traffic = 'NaN ' AND weather = 'NaN')::int) FROM orders"), 100 * patterns.null_block_share, 0.05),
        Check("store point (0, 0) %", pct("SELECT avg((store_lat = 0 AND store_lon = 0)::int) FROM orders"), 100 * patterns.store_zero_share, 0.4),
        Check("store latitude flipped %", pct("SELECT avg((store_lat < 0)::int) FROM orders"), 100 * patterns.lat_flip_share, 0.08),
        Check("pickup rollover %", pct("SELECT avg((pickup_time < order_time)::int) FROM orders WHERE order_time <> 'NaN '"), 1.89, 0.4),
    ]
    noop = pct(
        """SELECT avg((home_hub_id = lag_hub AND vehicle_type = lag_vehicle AND rating = lag_rating
                       AND employment_type = lag_employment)::int)
           FROM (SELECT *, lag(home_hub_id) OVER w lag_hub, lag(vehicle_type) OVER w lag_vehicle,
                        lag(rating) OVER w lag_rating, lag(employment_type) OVER w lag_employment
                 FROM courier_changes WINDOW w AS (PARTITION BY courier_id ORDER BY changed_ts, change_id)) t
           WHERE lag_hub IS NOT NULL"""
    )
    checks.append(Check("no-op audit rows (of changes after hiring) %", noop, 34.0, 5.0))
    mismatched = _one(
        cur,
        """SELECT count(*) FROM couriers c JOIN LATERAL (
               SELECT * FROM courier_changes x WHERE x.courier_id = c.courier_id ORDER BY changed_ts DESC, change_id DESC LIMIT 1) l ON true
           WHERE (c.home_hub_id, c.vehicle_type, c.rating, c.employment_type, c.updated_ts)
              <> (l.home_hub_id, l.vehicle_type, l.rating, l.employment_type, l.changed_ts)""",
    )
    checks.append(Check("couriers that differ from their last audit row", mismatched, 0, 0))

    facts["cardinality"] = {
        "orders.order_id": _one(cur, "SELECT count(DISTINCT order_id)::float / count(*) FROM orders"),
        "orders.customer_id": _one(cur, "SELECT count(DISTINCT customer_id)::float / count(*) FROM orders"),
        "orders.courier_id": _one(cur, "SELECT count(DISTINCT courier_id)::float / count(*) FROM orders"),
        "orders.(drop_lat, drop_lon)": _one(cur, "SELECT count(DISTINCT (drop_lat, drop_lon))::float / count(*) FROM orders"),
        "customers.customer_id": 1.0,
    }
    facts["hub skew (orders) %"] = {"Downtown LA": round(float(hub_counts.get(1, 0)), 2), "top 10": round(float(hub_counts.head(10).sum()), 2)}
    cur.execute("SELECT relname, pg_total_relation_size(relid) FROM pg_catalog.pg_statio_user_tables ORDER BY 2 DESC")
    facts["oms bytes"] = dict(cur.fetchall())
    facts["oms rows"] = counts
    cur.execute("SELECT hired_ts FROM couriers")
    hired = pd.Series([r[0] for r in cur.fetchall()])
    facts["active courier-days"] = int(sum((hired < d.tz_convert("UTC")).sum() for d in local_days()))
    cur.execute("SELECT order_id, courier_id FROM orders WHERE order_date = '2024-08-15'")
    facts["orders 2024-08-15"] = dict(cur.fetchall())


def _objects(s3, prefix: str) -> list[dict]:
    """Return every object under the prefix in the export bucket."""
    found = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=f"{prefix}/"):
        found += page.get("Contents", [])
    return found


def check_exports(scale: float, checks: list[Check], facts: dict) -> None:
    """Check the ping and scan exports in SeaweedFS and record their characteristics."""
    s3 = s3_client()
    per_day = defaultdict(lambda: [0, 0])
    rows = duplicates = idle = mega = 0
    trip_ids, trip_distinct, schemas, nbytes = 0, 0, {}, 0
    bad_trips = None
    for obj in (o for o in _objects(s3, PINGS) if o["Key"].endswith(".parquet")):
        body = s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read()
        table = pq.read_table(io.BytesIO(body), columns=["courier_id", "device_seq", "hub_id", "trip_id"])
        v2 = obj["Key"].endswith("-v2.parquet")
        schemas.setdefault("v2" if v2 else "v1", pq.read_schema(io.BytesIO(body)))
        date = obj["Key"].split("export_date=")[1][:10]
        distinct = table.group_by(["courier_id", "device_seq"]).aggregate([]).num_rows
        per_day[date][v2] += distinct
        rows, duplicates, nbytes = rows + table.num_rows, duplicates + table.num_rows - distinct, nbytes + obj["Size"]
        idle += table.column("trip_id").null_count
        mega += pc.sum(pc.equal(table.column("hub_id"), 1)).as_py() or 0
        trips = table.column("trip_id").drop_null()
        trip_ids, trip_distinct = trip_ids + len(trips), trip_distinct + len(pc.unique(trips))
        if date == "2024-08-15":
            frame = table.to_pandas().dropna(subset=["trip_id"])
            orders = facts["orders 2024-08-15"]
            found = frame.trip_id.astype(int).map(orders)
            bad_trips = (bad_trips or 0) + int((found != frame.courier_id).sum())
    originals = rows - duplicates
    checks += [
        Check("ping rows without duplicates", originals, facts["active courier-days"] * PINGS_PER_SHIFT, 0),
        Check("duplicate pings (% of originals)", 100 * duplicates / originals, 100 * DUPLICATE_SHARE, 0.01),
        Check("idle pings (NULL trip_id) %", 100 * idle / rows, 100 * IDLE_SHARE, 1.5),
        Check("pings from Downtown LA couriers %", 100 * mega / rows, 25.0, 4.0),
        Check("trip pings whose trip is another courier's order (2024-08-15)", bad_trips if bad_trips is not None else -1, 0, 0),
    ]
    share = {d: 100 * v[1] / sum(v) for d, v in sorted(per_day.items())}
    before = [s for d, s in share.items() if d < "2024-09-01"]
    after = [s for d, s in share.items() if d >= "2024-09-14"]
    rollout = [s for d, s in share.items() if "2024-09-01" <= d < "2024-09-14"]
    rising = all(a < b for a, b in zip(rollout, rollout[1:]))
    checks.append(Check("v2 share 0% before 09-01, rising, 100% from 09-14 (1 = yes)", float(max(before) == 0 and min(after) == 100 and rising), 1, 0))
    facts["v2 share by day %"] = {d: round(s, 1) for d, s in share.items() if "2024-08-31" <= d <= "2024-09-14"}
    facts["schemas"] = {k: [f"{f.name} {f.type}" for f in v] for k, v in schemas.items()}
    facts["pings"] = {"rows": rows, "duplicates": duplicates, "bytes": nbytes, "files": sum(1 for o in _objects(s3, PINGS) if o["Key"].endswith(".parquet"))}
    facts["cardinality"].update({
        "pings.(courier_id, device_seq)": originals / rows,
        "pings.trip_id (non-null)": trip_distinct / trip_ids,
    })

    scan_rows = scan_bytes = 0
    scan_ids = []
    for obj in (o for o in _objects(s3, SCANS) if o["Key"].endswith(".parquet")):
        table = pq.read_table(io.BytesIO(s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read()), columns=["scan_id"])
        scan_rows, scan_bytes = scan_rows + table.num_rows, scan_bytes + obj["Size"]
        scan_ids.append(table.column("scan_id"))
    distinct_scans = len(pc.unique(pa_concat(scan_ids)))
    checks += [
        Check("scans (3 per order)", scan_rows, 3 * facts["oms rows"]["orders"], 0),
        Check("distinct scan_id", distinct_scans, scan_rows, 0),
    ]
    facts["scans"] = {"rows": scan_rows, "bytes": scan_bytes}
    facts["cardinality"]["scans.scan_id"] = distinct_scans / scan_rows


def pa_concat(chunks):
    """Concatenate pyarrow arrays or chunked arrays into one chunked array."""
    return pa.chunked_array([c for chunk in chunks for c in (chunk.chunks if hasattr(chunk, "chunks") else [chunk])])


def main() -> None:
    """Run every check, print the results and the characteristics, and exit 1 on any failure."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--scale", type=float, default=0.01, help="the scale the data was generated at")
    args = parser.parse_args()
    checks: list[Check] = []
    facts: dict = {}
    with db.connect("oms") as conn, conn.cursor() as cur:
        check_oms(cur, args.scale, checks, facts)
    check_exports(args.scale, checks, facts)

    width = max(len(c.name) for c in checks)
    for c in checks:
        print(f"{'PASS' if c.passed else 'FAIL'}  {c.name:<{width}}  {c.measured:>14,.3f}  target {c.target:,.3f} ± {c.tolerance:g}")
    print()
    for key in ("late share by traffic %", "hub skew (orders) %", "v2 share by day %", "schemas", "pings", "scans", "oms rows"):
        print(f"{key}: {facts[key]}")
    print("cardinality (unique / total):", {k: round(v, 4) for k, v in facts["cardinality"].items()})
    print("oms bytes:", {k: f"{v / 1e6:.1f} MB" for k, v in facts["oms bytes"].items()})
    failed = [c.name for c in checks if not c.passed]
    print(f"\n{len(checks) - len(failed)} of {len(checks)} checks passed" + (f"; failed: {failed}" if failed else ""))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
