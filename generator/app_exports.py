"""Courier-app exports in SeaweedFS: GPS pings and parcel scans as daily Parquet files.

    uv run python -m generator.app_exports --scale 0.01 --workers 6

Files go to s3://app-exports/courier_pings/ and s3://app-exports/parcel_scans/, in one
export_date=YYYY-MM-DD folder per local day. Each file is built in memory and uploaded with one PUT,
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
APP_VERSION_V1 = "3.14.2"
UPLOAD_BATCH_S = 300  # phones upload their pings every 5 minutes
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


def build_ping_file(
    day: pd.Timestamp, day_index: int, couriers: pd.DataFrame, trips: dict[int, np.ndarray], rng: np.random.Generator
) -> pa.Table:
    """Return one file of v1 pings: a 10-hour shift every 25 s for each courier in couriers.

    couriers holds courier_id, home_hub_id and vehicle_type as valid that day; trips maps a
    courier to that day's order ids in pickup order, which become the trip ids.
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

    return pa.table(
        {
            "courier_id": np.repeat(ids, p).astype(np.int32),
            "device_seq": (day_index * p + np.tile(np.arange(p), m)).astype(np.int64),
            "hub_id": np.repeat(couriers.home_hub_id.to_numpy(), p).astype(np.int16),
            "trip_id": pa.array(trip_id.ravel(), mask=~has_trip.ravel()),
            "ping_ts": pa.array(ping_ns.ravel() // 1_000_000, type=pa.timestamp("ms", tz="UTC")),
            "lat": lat.ravel().round(6),
            "lon": lon.ravel().round(6),
            "speed_kmh": speed.ravel().round().astype(np.int16),
            "app_version": pa.array([APP_VERSION_V1] * (m * p)),
            "uploaded_ts": pa.array(uploaded_ns.ravel() // 1_000_000, type=pa.timestamp("ms", tz="UTC")),
        },
        schema=PING_SCHEMA,
    )


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
    """Build and upload one ping file (runs in a worker process)."""
    rng = np.random.default_rng([seed_value, 4, day_index, part])
    table = build_ping_file(day, day_index, couriers, trips, rng)
    return _upload(f"{PINGS}/export_date={day.date()}/part-{part:05d}.parquet", parquet_bytes(table))


def main() -> None:
    """Generate and upload the ping and scan exports for the whole period."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--scale", type=float, default=0.01, help="must match the scale of the oms load")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="must match the seed of the oms load")
    parser.add_argument("--workers", type=int, default=4, help="processes building ping files")
    parser.add_argument("--days", type=int, default=None, help="only the first N days (for checks)")
    args = parser.parse_args()
    started = time.perf_counter()
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
        manifest += [f.result() for f in pending]
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
