"""Courier-app exports in SeaweedFS: GPS pings and parcel scans as daily Parquet files.

    uv run python -m generator.app_exports --scale 0.01 --workers 6

Files go to s3://app-exports/courier_pings/ and s3://app-exports/parcel_scans/, in one
export_date=YYYY-MM-DD folder per local day. From 2024-09-01, app v2 phones write a second ping
schema next to v1 in the same folders, and 2.00% of ping rows are re-uploaded duplicates. Each file is built in memory and uploaded with one PUT,
so nothing is stored on the Mac. Every file has its own random seed, so a rerun writes
byte-identical files. The orders are rebuilt from the same seed as the oms load, so every scan and
trip matches a row in oms.orders. Rules and rates are in docs/data_generator.md.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import time
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from scipy.signal import lfilter

from generator import db
from generator.la_config import TIME_ZONE
from generator.la_hubs import hubs_frame
from generator.oms_orders import build_orders, courier_state_at, local_days, read_reference
from generator.oms_reference import DEFAULT_SEED
from generator.storage import s3_client

BUCKET = "app-exports"
PINGS, SCANS = "courier_pings", "parcel_scans"
PING_INTERVAL_S = 25
PINGS_PER_SHIFT = 10 * 3600 // PING_INTERVAL_S  # a 10-hour shift: 1,440 pings
SHIFT_START_HOURS = (7, 14)  # local; couriers with an even id work 07-17, odd ids 14-24
COURIERS_PER_FILE = 278  # 278 x 1,440 = 400,320 pings per file
IDLE_SHARE = 0.30  # pings between trips carry a NULL trip_id
MEAN_TRIP_PINGS = 72  # 30 minutes
VEHICLE_SPEED_KMH = {"van": 24.0, "car": 28.0, "e-bike": 16.0}  # mean speed while on a trip
POSITION_PULL = 0.995  # per ping, toward the hub: median 5.8 km away, 95% within 11.4 km
GPS_NOISE_DEG = 0.00007  # about 8 m
APP_VERSION_V1, APP_VERSION_V2 = "3.14.2", "4.0.0"
UPLOAD_BATCH_S = 300  # phones upload their pings every 5 minutes
PINGS_PER_BATCH = UPLOAD_BATCH_S // PING_INTERVAL_S  # 12
# App v2 reaches every phone over 14 days from 2024-09-01: each courier's phone updates on one of
# those days, so the v2 share rises from 0 to 100% and both schemas share the export folders.
V2_ROLLOUT_START = pd.Timestamp("2024-09-01", tz=TIME_ZONE)
V2_ROLLOUT_DAYS = 14
# Phones re-upload some 5-minute batches: 2.00% extra rows, same (courier_id, device_seq) and
# content, a later uploaded_ts, the same export_date.
DUPLICATE_SHARE = 0.02
REUPLOAD_DELAY_S = (10 * 60, 60 * 60)
BATTERY_DRAIN_PER_HOUR = 6.5
SCAN_TYPES = ("at_hub", "picked_up", "delivered")
NS = 1_000_000_000

PING_SCHEMA = pa.schema(
    [
        ("courier_id", pa.int32()), ("device_seq", pa.int64()), ("hub_id", pa.int16()),
        ("trip_id", pa.int64()), ("ping_ts", pa.timestamp("ms", tz="UTC")), ("lat", pa.float64()),
        ("lon", pa.float64()), ("speed_kmh", pa.int16()), ("app_version", pa.string()),
        ("uploaded_ts", pa.timestamp("ms", tz="UTC")),
    ]
)
# v2 replaces speed_kmh (int) with speed_mps (float) and adds battery_pct and gps_accuracy_m.
PING_SCHEMA_V2 = pa.schema(
    [
        ("courier_id", pa.int32()), ("device_seq", pa.int64()), ("hub_id", pa.int16()),
        ("trip_id", pa.int64()), ("ping_ts", pa.timestamp("ms", tz="UTC")), ("lat", pa.float64()),
        ("lon", pa.float64()), ("speed_mps", pa.float32()), ("battery_pct", pa.int8()),
        ("gps_accuracy_m", pa.float32()), ("app_version", pa.string()),
        ("uploaded_ts", pa.timestamp("ms", tz="UTC")),
    ]
)
SCAN_SCHEMA = pa.schema(
    [
        ("scan_id", pa.int64()), ("order_id", pa.int64()), ("courier_id", pa.int32()), ("hub_id", pa.int16()),
        ("scan_type", pa.string()), ("scan_ts", pa.timestamp("ms", tz="UTC")),
        ("uploaded_ts", pa.timestamp("ms", tz="UTC")),
    ]
)


def _shift_start_ns(day: pd.Timestamp, courier_ids: np.ndarray) -> np.ndarray:
    """Return each courier's shift start that local day as UTC nanoseconds (wall-clock hours)."""
    starts = {
        h: (day.tz_localize(None) + pd.Timedelta(hours=h)).tz_localize(TIME_ZONE).tz_convert("UTC").value
        for h in SHIFT_START_HOURS
    }
    return np.array([starts[SHIFT_START_HOURS[c % 2]] for c in courier_ids], dtype=np.int64)


def _segments(rng: np.random.Generator, m: int) -> tuple[np.ndarray, np.ndarray]:
    """Return (in_trip, trip_number) arrays of shape (m, PINGS_PER_SHIFT): alternating trips and idle gaps."""
    mean_idle = MEAN_TRIP_PINGS * IDLE_SHARE / (1 - IDLE_SHARE)
    segments = 2 * (PINGS_PER_SHIFT // (MEAN_TRIP_PINGS // 4))
    in_trip = np.zeros((m, PINGS_PER_SHIFT), dtype=bool)
    number = np.zeros((m, PINGS_PER_SHIFT), dtype=np.int64)
    for row in range(m):
        lengths = np.column_stack(
            [rng.geometric(1 / MEAN_TRIP_PINGS, segments // 2), rng.geometric(1 / mean_idle, segments // 2)]
        ).ravel()
        flags = np.repeat(np.tile([True, False], segments // 2), lengths)[:PINGS_PER_SHIFT]
        numbers = np.repeat(np.repeat(np.arange(segments // 2), 2), lengths)[:PINGS_PER_SHIFT]
        in_trip[row, : len(flags)], number[row, : len(numbers)] = flags, numbers
    return in_trip, number


def runs_v2(day: pd.Timestamp, courier_ids) -> np.ndarray:
    """Return True for each courier whose phone runs app v2 on that local day.

    Each phone updates on rollout day (courier_id * 7919) % 14, which spreads couriers evenly
    over the 14 days and is the same in every file and every run.
    """
    days_in = (day - V2_ROLLOUT_START).days
    return (np.asarray(courier_ids, dtype=np.int64) * 7_919) % V2_ROLLOUT_DAYS <= days_in


def build_ping_files(
    day: pd.Timestamp, day_index: int, couriers: pd.DataFrame, trips: dict[int, np.ndarray], rng: np.random.Generator
) -> list[tuple[str, pa.Table]]:
    """Return one day's pings for a group of couriers as (file suffix, table) pairs.

    Every courier sends a fix every 25 s through a 10-hour shift. couriers holds courier_id,
    home_hub_id and vehicle_type as valid that day; trips maps a courier to that day's order ids in
    pickup order, which become the trip ids. Couriers on app v1 go to the "" file and couriers on
    v2 to the "-v2" file, because one Parquet file has one schema. 2.00% extra rows are re-uploaded
    5-minute batches.
    """
    m, p = len(couriers), PINGS_PER_SHIFT
    ids = couriers.courier_id.to_numpy()
    hub = hubs_frame().set_index("hub_id").loc[couriers.home_hub_id.to_numpy()]
    ping_ns = _shift_start_ns(day, ids)[:, None] + np.arange(p, dtype=np.int64) * PING_INTERVAL_S * NS

    in_trip, number = _segments(rng, m)
    trip_id = np.zeros((m, p), dtype=np.int64)
    has_trip = np.zeros((m, p), dtype=bool)
    for row, courier in enumerate(ids):
        orders = trips.get(int(courier))
        if orders is not None and len(orders):
            trip_id[row] = orders[number[row] % len(orders)]
            has_trip[row] = in_trip[row]

    speed = np.where(
        has_trip,
        couriers.vehicle_type.map(VEHICLE_SPEED_KMH).to_numpy()[:, None] * rng.lognormal(0, 0.3, (m, p)),
        rng.uniform(0, 2, (m, p)),
    )
    heading = rng.uniform(0, 2 * np.pi, (m, 1)) + np.cumsum(rng.normal(0, 0.35, (m, p)), axis=1)
    step_km = speed * PING_INTERVAL_S / 3600
    north_km = lfilter([1.0], [1.0, -POSITION_PULL], step_km * np.cos(heading), axis=1)
    east_km = lfilter([1.0], [1.0, -POSITION_PULL], step_km * np.sin(heading), axis=1)
    hub_lat, hub_lon = hub.lat.to_numpy()[:, None], hub.lon.to_numpy()[:, None]
    lat = hub_lat + north_km / 111.32 + rng.normal(0, GPS_NOISE_DEG, (m, p))
    lon = hub_lon + east_km / (111.32 * np.cos(np.radians(hub_lat))) + rng.normal(0, GPS_NOISE_DEG, (m, p))
    uploaded_ns = (ping_ns // (UPLOAD_BATCH_S * NS) + 1) * UPLOAD_BATCH_S * NS + rng.integers(1, 30, (m, p)) * NS
    shift_hours = np.arange(p) * PING_INTERVAL_S / 3600
    battery = rng.uniform(85, 100, (m, 1)) - BATTERY_DRAIN_PER_HOUR * shift_hours + rng.normal(0, 1, (m, p))
    accuracy = rng.lognormal(np.log(5), 0.4, (m, p))

    rows = {
        "courier_id": np.repeat(ids, p).astype(np.int32),
        "device_seq": (day_index * p + np.tile(np.arange(p), m)).astype(np.int64),
        "hub_id": np.repeat(couriers.home_hub_id.to_numpy(), p).astype(np.int16),
        "trip_id": trip_id.ravel(),
        "has_trip": has_trip.ravel(),
        "ping_ms": ping_ns.ravel() // 1_000_000,
        "lat": lat.ravel().round(6),
        "lon": lon.ravel().round(6),
        "speed_kmh": speed.ravel(),
        "battery_pct": np.clip(battery, 5, 100).ravel().round().astype(np.int8),
        "gps_accuracy_m": accuracy.ravel().round(1).astype(np.float32),
        "uploaded_ms": uploaded_ns.ravel() // 1_000_000,
        "v2": np.repeat(runs_v2(day, ids), p),
    }
    batches = m * (p // PINGS_PER_BATCH)
    chosen = rng.choice(batches, round(DUPLICATE_SHARE * batches), replace=False)
    again = (chosen[:, None] * PINGS_PER_BATCH + np.arange(PINGS_PER_BATCH)).ravel()
    delay_ms = np.repeat(rng.integers(*REUPLOAD_DELAY_S, len(chosen)), PINGS_PER_BATCH) * 1000
    copies = {k: v[again] for k, v in rows.items()}
    copies["uploaded_ms"] = copies["uploaded_ms"] + delay_ms
    rows = {k: np.concatenate([rows[k], copies[k]]) for k in rows}

    files = []
    for suffix, keep in (("", ~rows["v2"]), ("-v2", rows["v2"])):
        if keep.any():
            files.append((suffix, _ping_table({k: v[keep] for k, v in rows.items()}, v2=suffix == "-v2")))
    return files


def _ping_table(rows: dict[str, np.ndarray], v2: bool) -> pa.Table:
    """Return the ping rows in the v1 or v2 Parquet schema."""
    n = len(rows["courier_id"])
    ms = pa.timestamp("ms", tz="UTC")
    columns = {
        "courier_id": rows["courier_id"],
        "device_seq": rows["device_seq"],
        "hub_id": rows["hub_id"],
        "trip_id": pa.array(rows["trip_id"], mask=~rows["has_trip"]),
        "ping_ts": pa.array(rows["ping_ms"], type=ms),
        "lat": rows["lat"],
        "lon": rows["lon"],
    }
    if v2:
        columns["speed_mps"] = (rows["speed_kmh"] / 3.6).round(2).astype(np.float32)
        columns["battery_pct"] = rows["battery_pct"]
        columns["gps_accuracy_m"] = rows["gps_accuracy_m"]
    else:
        columns["speed_kmh"] = rows["speed_kmh"].round().astype(np.int16)
    columns["app_version"] = pa.array([APP_VERSION_V2 if v2 else APP_VERSION_V1] * n)
    columns["uploaded_ts"] = pa.array(rows["uploaded_ms"], type=ms)
    return pa.table(columns, schema=PING_SCHEMA_V2 if v2 else PING_SCHEMA)


def build_scans(orders: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """Return three scans per order: at_hub (by hub staff, no courier), picked_up and delivered."""
    n = len(orders)
    order_ns, pickup_ns, delivered_ns = (
        orders[c].dt.as_unit("ns").astype("int64").to_numpy() for c in ("order_ts", "pickup_ts", "delivered_ts")
    )
    gap_s = (pickup_ns - order_ns) // NS
    at_hub_ns = order_ns + (30 + (rng.random(n) * (gap_s - 60))).astype(np.int64) * NS
    scan_ns = np.column_stack([at_hub_ns, pickup_ns, delivered_ns]).ravel()
    order_id = np.repeat(orders.order_id.to_numpy(), 3)
    courier = np.column_stack([np.full(n, -1), orders.courier_id, orders.courier_id]).ravel()
    return pd.DataFrame(
        {
            "scan_id": order_id * 3 - 2 + np.tile(np.arange(3), n),
            "order_id": order_id,
            "courier_id": pd.array(np.where(courier < 0, pd.NA, courier), dtype="Int32"),
            "hub_id": np.repeat(orders.hub_id.to_numpy(), 3).astype(np.int16),
            "scan_type": np.tile(SCAN_TYPES, n),
            "scan_ns": scan_ns,
            "uploaded_ns": scan_ns + rng.integers(1, 120, 3 * n) * NS,
        }
    )


def _scan_table(scans: pd.DataFrame) -> pa.Table:
    """Convert a scans frame to the Parquet schema."""
    return pa.table(
        {
            "scan_id": scans.scan_id.to_numpy(),
            "order_id": scans.order_id.to_numpy(),
            "courier_id": pa.array(scans.courier_id, type=pa.int32(), from_pandas=True),
            "hub_id": scans.hub_id.to_numpy(),
            "scan_type": scans.scan_type.to_numpy(),
            "scan_ts": pa.array(scans.scan_ns.to_numpy() // 1_000_000, type=pa.timestamp("ms", tz="UTC")),
            "uploaded_ts": pa.array(scans.uploaded_ns.to_numpy() // 1_000_000, type=pa.timestamp("ms", tz="UTC")),
        },
        schema=SCAN_SCHEMA,
    )


def parquet_bytes(table: pa.Table) -> bytes:
    """Serialise a table as one zstd Parquet file (one row group), byte-identical for identical tables."""
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="zstd", row_group_size=max(1, table.num_rows))
    return buffer.getvalue()


_S3 = None


def _upload(key: str, body: bytes) -> tuple[str, int, int, str]:
    """PUT one object (a single PUT, so its ETag is the MD5) and return (key, rows, bytes, md5)."""
    global _S3
    if _S3 is None:
        _S3 = s3_client()
    _S3.put_object(Bucket=BUCKET, Key=key, Body=body)
    rows = pq.ParquetFile(io.BytesIO(body)).metadata.num_rows if key.endswith(".parquet") else 0
    return key, rows, len(body), hashlib.md5(body).hexdigest()


def _ping_task(day: pd.Timestamp, day_index: int, part: int, couriers: pd.DataFrame, trips: dict, seed_value: int):
    """Build and upload one group of couriers' ping files for one day (runs in a worker process)."""
    rng = np.random.default_rng([seed_value, 4, day_index, part])
    return [
        _upload(f"{PINGS}/export_date={day.date()}/part-{part:05d}{suffix}.parquet", parquet_bytes(table))
        for suffix, table in build_ping_files(day, day_index, couriers, trips, rng)
    ]


def _existing_keys(prefixes: tuple[str, ...]) -> list[str]:
    """Return every object key in the bucket under the given prefixes."""
    s3, keys = s3_client(), []
    for prefix in prefixes:
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=f"{prefix}/"):
            keys += [o["Key"] for o in page.get("Contents", [])]
    return keys


def _delete(keys: list[str]) -> None:
    """Delete objects from the bucket, 1,000 per request."""
    s3 = s3_client()
    for start in range(0, len(keys), 1000):
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": [{"Key": k} for k in keys[start:start + 1000]]})


def main() -> None:
    """Generate and upload the ping and scan exports for the whole period."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--scale", type=float, default=0.01, help="must match the scale of the oms load")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="must match the seed of the oms load")
    parser.add_argument("--workers", type=int, default=4, help="processes building ping files")
    parser.add_argument("--days", type=int, default=None, help="only the first N days (for checks)")
    parser.add_argument("--replace", action="store_true", help="delete the existing ping and scan exports first")
    args = parser.parse_args()
    started = time.perf_counter()
    existing = _existing_keys((PINGS, SCANS))
    if existing and not args.replace:
        raise SystemExit(f"{BUCKET} already holds {len(existing):,} export objects; run again with --replace")
    _delete(existing)
    with db.connect("oms") as conn, conn.cursor() as cur:
        reference = read_reference(cur)
        cur.execute("SELECT count(*) FROM orders")
        oms_orders = cur.fetchone()[0]
    days = local_days()
    manifest, generated, scan_parts = [], 0, {}
    with ProcessPoolExecutor(args.workers) as pool:
        pending = []
        for i, orders in enumerate(build_orders(args.scale, reference, args.seed, days=args.days)):
            day, generated = days[i], generated + len(orders)
            scans = build_scans(orders, np.random.default_rng([args.seed, 5, i]))
            export_date = pd.to_datetime(scans.scan_ns, utc=True).dt.tz_convert(TIME_ZONE).dt.date
            for date, part in scans.groupby(export_date.to_numpy()):
                key = f"{SCANS}/export_date={date}/orders-{day.date()}.parquet"
                manifest.append(_upload(key, parquet_bytes(_scan_table(part.sort_values("scan_ns")))))
                scan_parts.setdefault(str(date), []).append(key)
            state = courier_state_at(reference["courier_changes"], reference["couriers"], day.tz_convert("UTC"))
            state = state.sort_values("courier_id").reset_index(drop=True)
            trips = {int(c): g.order_id.to_numpy() for c, g in orders.sort_values("pickup_ts").groupby("courier_id")}
            for part, start in enumerate(range(0, len(state), COURIERS_PER_FILE)):
                chunk = state.iloc[start:start + COURIERS_PER_FILE][["courier_id", "home_hub_id", "vehicle_type"]]
                chunk_trips = {c: trips[c] for c in chunk.courier_id if c in trips}
                pending.append(pool.submit(_ping_task, day, i, part, chunk, chunk_trips, args.seed))
        manifest += [entry for f in pending for entry in f.result()]
    finished = sorted({k.split("/")[1] for k, *_ in manifest if k.startswith(PINGS)})
    for folder in finished:
        manifest.append(_upload(f"{PINGS}/{folder}/_SUCCESS", b""))
    for date in sorted(scan_parts)[: len(finished) - 1 if args.days else None]:
        manifest.append(_upload(f"{SCANS}/export_date={date}/_SUCCESS", b""))
    if args.days is None and generated != oms_orders:
        raise SystemExit(f"rebuilt {generated:,} orders but oms holds {oms_orders:,}: use the scale and seed of the oms load")
    pings = [m for m in manifest if m[0].startswith(PINGS) and m[1]]
    scans_files = [m for m in manifest if m[0].startswith(SCANS) and m[1]]
    for name, files in (("pings", pings), ("scans", scans_files)):
        rows, size = sum(f[1] for f in files), sum(f[2] for f in files)
        print(f"{name}: {len(files):,} files, {rows:,} rows, {size / 1e9:.2f} GB, {size / max(rows, 1):.1f} B/row")
    print(f"manifest sha256 {hashlib.sha256(''.join(sorted(f'{k}:{h}' for k, _, _, h in manifest)).encode()).hexdigest()[:16]}")
    print(f"done in {time.perf_counter() - started:.0f} s")


if __name__ == "__main__":
    main()
