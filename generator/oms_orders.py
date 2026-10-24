"""Orders of the order system, generated one local day at a time and loaded into Postgres oms.

    uv run python -m generator.oms_orders --scale 0.01

Each order gets a customer (and so the customer's hub, which carries the skew), a courier from
that hub, traffic and weather from the LA mix, and a delivery time from the seed effect model.
The table then stores it the way the seed does: local date and time text, legacy spellings,
"NaN" literals and broken store coordinates, at the seed's own rates (docs/data_generator.md).
"""

from __future__ import annotations

import argparse
import io
import time
from dataclasses import dataclass

import numpy as np
import pandas as pd

from generator import db
from generator.effect_model import SEED_PATH, EffectModel, fit, haversine_km, load_seed, sla_minutes
from generator.la_config import (
    FULL_ORDERS_PER_DAY,
    PEAK_WEEKS,
    PERIOD_END,
    PERIOD_START,
    TIME_ZONE,
    TRAFFIC_MIX,
    WEATHER_MIX,
)
from generator.la_hubs import hubs_frame
from generator.oms_reference import DEFAULT_SEED, SCHEMA_SQL

ORDER_COLUMNS = [
    "order_id", "customer_id", "hub_id", "courier_id", "category", "order_date", "order_time",
    "pickup_time", "promised_ts", "delivered_ts", "status", "weather", "traffic", "vehicle", "area",
    "store_lat", "store_lon", "drop_lat", "drop_lon", "updated_ts",
]
# How the legacy order system spells each value, copied from the seed: every traffic value and
# every vehicle and area except "van" and "Other" carry a trailing space; Metropolitan is misspelled.
LEGACY_TRAFFIC = {v: f"{v} " for v in TRAFFIC_MIX}
LEGACY_VEHICLE = {"van": "van", "car": "car ", "e-bike": "e-bike "}
LEGACY_AREA = {"Metropolitan": "Metropolitian ", "Urban": "Urban ", "Semi-Urban": "Semi-Urban ", "Other": "Other"}
NULL_TEXT = {"traffic": "NaN ", "weather": "NaN", "order_time": "NaN "}
MIN_DELIVERY_MIN = 10.0  # the seed's shortest delivery


@dataclass(frozen=True)
class SeedPatterns:
    """What the orders copy from the raw seed: time of day, pickup delay, categories and dirty rates."""

    minute_of_day: np.ndarray
    pickup_delay_min: np.ndarray
    categories: np.ndarray
    null_block_share: float
    store_zero_share: float
    lat_flip_share: float
    lon_flip_given_lat_flip: float


def seed_patterns() -> SeedPatterns:
    """Measure the patterns in the raw seed file."""
    raw = pd.read_csv(SEED_PATH, dtype=str, keep_default_na=False)
    null_block = raw.Order_Time.str.strip() == "NaN"
    ordered = pd.to_datetime(raw.Order_Time[~null_block], format="%H:%M:%S")
    picked = pd.to_datetime(raw.Pickup_Time[~null_block], format="%H:%M:%S")
    delay = ((picked - ordered).dt.total_seconds() / 60) % (24 * 60)
    lat, lon = pd.to_numeric(raw.Store_Latitude), pd.to_numeric(raw.Store_Longitude)
    return SeedPatterns(
        minute_of_day=(ordered.dt.hour * 60 + ordered.dt.minute).to_numpy(),
        pickup_delay_min=delay.to_numpy(),
        categories=raw.Category.str.strip().to_numpy(),
        null_block_share=float(null_block.mean()),
        store_zero_share=float(((lat == 0) & (lon == 0)).mean()),
        lat_flip_share=float((lat < 0).mean()),
        lon_flip_given_lat_flip=float(((lon < 0) & (lat < 0)).sum() / (lat < 0).sum()),
    )


def local_days() -> pd.DatetimeIndex:
    """Return the local LA dates of the period (midnight, tz-aware)."""
    return pd.date_range(PERIOD_START.tz_convert(TIME_ZONE), PERIOD_END.tz_convert(TIME_ZONE), freq="D", inclusive="left")


def orders_per_day(scale: float) -> pd.Series:
    """Return the number of orders for each local day, with the peak-week factors applied."""
    days = local_days()
    factor = pd.Series(1.0, index=days)
    for first, last, peak in PEAK_WEEKS:
        factor[(days >= pd.Timestamp(first, tz=TIME_ZONE)) & (days <= pd.Timestamp(last, tz=TIME_ZONE))] = peak
    return (factor * FULL_ORDERS_PER_DAY * scale).round().astype(int)


def courier_state_at(audit: pd.DataFrame, couriers: pd.DataFrame, instant: pd.Timestamp) -> pd.DataFrame:
    """Return each courier hired before instant with the attributes valid at that instant."""
    hired = couriers.loc[couriers.hired_ts < instant, "courier_id"]
    valid = audit[(audit.changed_ts <= instant) & audit.courier_id.isin(hired)]
    return valid.sort_values("change_id").groupby("courier_id").tail(1).reset_index(drop=True)


def _local_clock(ts: pd.Series) -> pd.Series:
    """Format UTC instants as local LA wall-clock text, HH:MM:SS."""
    return ts.dt.tz_convert(TIME_ZONE).dt.strftime("%H:%M:%S")


def build_day(
    day: pd.Timestamp,
    n: int,
    first_order_id: int,
    reference: dict[str, pd.DataFrame],
    model: EffectModel,
    patterns: SeedPatterns,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """Return the n orders placed on one local day, as the order system stores them."""
    hubs = hubs_frame().set_index("hub_id")
    customers = reference["customers"]
    order_ts = day + pd.to_timedelta(rng.choice(patterns.minute_of_day, n) * 60 + rng.integers(0, 300, n), unit="s")
    order_ts = pd.Series(order_ts.tz_convert("UTC"))

    eligible = customers[customers.signup_ts < day.tz_convert("UTC")]
    customer = eligible.iloc[rng.integers(0, len(eligible), n)].reset_index(drop=True)
    hub_id = customer.service_hub_id.to_numpy()
    hub = hubs.loc[hub_id]

    state = courier_state_at(reference["courier_changes"], reference["couriers"], day.tz_convert("UTC"))
    by_hub = {h: g.index.to_numpy() for h, g in state.groupby("home_hub_id")}
    everyone = state.index.to_numpy()
    pick = np.empty(n, dtype=int)
    for h in np.unique(hub_id):
        rows = np.flatnonzero(hub_id == h)
        pool = by_hub.get(h, everyone)  # a hub with no courier that day borrows one from any hub
        pick[rows] = rng.choice(pool, len(rows))
    courier = state.loc[pick].reset_index(drop=True)

    traffic = rng.choice(list(TRAFFIC_MIX), n, p=list(TRAFFIC_MIX.values()))
    weather = rng.choice(list(WEATHER_MIX), n, p=list(WEATHER_MIX.values()))
    area = hub.area_type.to_numpy()
    km = haversine_km(hub.lat.to_numpy(), hub.lon.to_numpy(), customer.home_lat, customer.home_lon)
    delay = rng.choice(patterns.pickup_delay_min, n)
    minutes = model.sample_minutes(rng, traffic, weather, courier.vehicle_type, area, km, courier.rating)
    minutes = np.maximum(minutes, np.maximum(MIN_DELIVERY_MIN, delay + 5))
    pickup_ts = order_ts + pd.to_timedelta(delay, unit="min")

    orders = pd.DataFrame(
        {
            "order_id": np.arange(first_order_id, first_order_id + n),
            "customer_id": customer.customer_id,
            "hub_id": hub_id,
            "courier_id": courier.courier_id,
            "category": rng.choice(patterns.categories, n),
            "order_date": order_ts.dt.tz_convert(TIME_ZONE).dt.date,
            "order_time": _local_clock(order_ts),
            "pickup_time": _local_clock(pickup_ts),
            "promised_ts": (order_ts + pd.to_timedelta(sla_minutes(area, km), unit="min")).dt.round("s"),
            "delivered_ts": (order_ts + pd.to_timedelta(minutes, unit="min")).dt.round("s"),
            "status": "delivered",
            "weather": weather,
            "traffic": pd.Series(traffic).map(LEGACY_TRAFFIC),
            "vehicle": courier.vehicle_type.map(LEGACY_VEHICLE),
            "area": pd.Series(area).map(LEGACY_AREA),
            "store_lat": hub.lat.to_numpy(),
            "store_lon": hub.lon.to_numpy(),
            "drop_lat": customer.home_lat,
            "drop_lon": customer.home_lon,
        }
    )
    orders["updated_ts"] = orders.delivered_ts
    # Number the orders in time order. Sort by the instant, not the local text, which repeats an
    # hour when daylight saving time ends (2024-11-03).
    orders = orders.iloc[np.argsort(order_ts.to_numpy(), kind="stable")].reset_index(drop=True)
    orders["order_id"] = np.arange(first_order_id, first_order_id + n)
    return _inject_dirty_values(orders, patterns, rng)


def _inject_dirty_values(orders: pd.DataFrame, patterns: SeedPatterns, rng: np.random.Generator) -> pd.DataFrame:
    """Write the seed's null block and broken store coordinates into a share of the rows."""
    n = len(orders)
    null_block = rng.random(n) < patterns.null_block_share
    for column, text in NULL_TEXT.items():
        orders.loc[null_block, column] = text
    broken = rng.random(n)
    zero = broken < patterns.store_zero_share
    lat_flip = (broken >= patterns.store_zero_share) & (broken < patterns.store_zero_share + patterns.lat_flip_share)
    lon_flip = lat_flip & (rng.random(n) < patterns.lon_flip_given_lat_flip)
    orders.loc[zero, ["store_lat", "store_lon"]] = 0.0
    orders.loc[lat_flip, "store_lat"] *= -1
    orders.loc[lon_flip, "store_lon"] *= -1
    return orders


def build_orders(scale: float, reference: dict[str, pd.DataFrame], seed_value: int = DEFAULT_SEED, days: int | None = None):
    """Yield one DataFrame of orders per local day; each day has its own random seed."""
    seed = load_seed()
    model, patterns = fit(seed), seed_patterns()
    counts = orders_per_day(scale)
    first_ids = 1 + counts.cumsum().shift(fill_value=0)
    for i, (day, n) in enumerate(counts.items()):
        if days is not None and i >= days:
            return
        rng = np.random.default_rng([seed_value, 3, i])
        yield build_day(day, int(n), int(first_ids.iloc[i]), reference, model, patterns, rng)


def _read_table(cur, query: str) -> pd.DataFrame:
    """Read a query result with COPY into a DataFrame (fast for millions of rows)."""
    buffer = io.StringIO()
    with cur.copy(f"COPY ({query}) TO STDOUT (FORMAT csv, HEADER)") as copy:
        for block in copy:
            buffer.write(bytes(block).decode())
    buffer.seek(0)
    return pd.read_csv(buffer)


def read_reference(cur) -> dict[str, pd.DataFrame]:
    """Read the couriers, their audit and the customers that orders point to from oms."""
    reference = {
        "couriers": _read_table(cur, "SELECT courier_id, hired_ts FROM couriers"),
        "courier_changes": _read_table(cur, "SELECT change_id, courier_id, changed_ts, home_hub_id, vehicle_type, rating FROM courier_changes"),
        "customers": _read_table(cur, "SELECT customer_id, service_hub_id, home_lat, home_lon, signup_ts FROM customers ORDER BY customer_id"),
    }
    for frame, column in [("couriers", "hired_ts"), ("courier_changes", "changed_ts"), ("customers", "signup_ts")]:
        reference[frame][column] = pd.to_datetime(reference[frame][column], utc=True, format="ISO8601")
    if not len(reference["customers"]):
        raise SystemExit("oms has no reference data; run python -m generator.oms_reference first")
    return reference


def main() -> None:
    """Generate the orders day by day and COPY each day into oms.orders."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--scale", type=float, default=0.01, help="1.0 = 400k orders a day; 0.01 = the dev slice")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--replace", action="store_true", help="delete the orders already in oms first")
    args = parser.parse_args()
    started = time.perf_counter()
    total = 0
    with db.connect("oms") as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_SQL.read_text())
        cur.execute("SELECT EXISTS (SELECT 1 FROM orders)")
        if cur.fetchone()[0] and not args.replace:
            raise SystemExit("oms.orders already holds rows; run again with --replace to overwrite them")
        cur.execute("TRUNCATE orders")
        reference = read_reference(cur)
        for orders in build_orders(args.scale, reference, args.seed):
            db.copy_frame(cur, "orders", orders[ORDER_COLUMNS])
            total += len(orders)
    print(f"orders {total:,} loaded in {time.perf_counter() - started:.1f} s")


if __name__ == "__main__":
    main()
