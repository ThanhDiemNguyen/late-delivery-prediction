"""Reference data of the order system: hubs, couriers with their change history, and customers.

Build it in memory with build_reference(), or build it and load it into the Postgres oms database:

    uv run python -m generator.oms_reference --scale 0.01

The same scale and seed always give the same rows. The rules and rates are in docs/data_generator.md.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import numpy as np
import pandas as pd

from generator import db
from generator.effect_model import EARTH_RADIUS_KM, load_seed
from generator.la_config import FULL_COURIERS, FULL_CUSTOMERS, PERIOD_END, PERIOD_START, VEHICLE_MIX
from generator.la_hubs import hubs_frame, inside_service_area

SCHEMA_SQL = Path(__file__).resolve().parent / "sql" / "oms_schema.sql"
DEFAULT_SEED = 2024
TABLES = ("hubs", "couriers", "courier_changes", "customers")
TRACKED = ["home_hub_id", "vehicle_type", "rating", "employment_type"]

HUBS_OPENED = pd.Timestamp("2020-01-01", tz="UTC")
COURIER_HIRE_FROM = pd.Timestamp("2022-01-01", tz="UTC")
CUSTOMER_SIGNUP_FROM = pd.Timestamp("2019-01-01", tz="UTC")
LAST_HIRE_BEFORE_END = pd.Timedelta(days=14)
HIRED_DURING_PERIOD = 0.20
SIGNED_UP_DURING_PERIOD = 0.15
MIN_COURIER_AGE = 18
EMPLOYEE_SHARE = {"van": 0.85, "car": 0.10, "e-bike": 0.40}  # the rest are contractors
RATING_REVIEWS_PER_PERIOD = 4.5  # Poisson mean for a courier employed the whole period
RATING_STEPS = {-0.1: 0.35, 0.0: 0.30, 0.1: 0.35}  # a 0.0 step is a review that changes nothing
TRANSFER_SHARE = 0.08
VEHICLE_CHANGE_SHARE = 0.05
EMPLOYMENT_CHANGE_SHARE = 0.03
SEGMENT_MIX = {"consumer": 0.88, "small_business": 0.12}
MAX_PLACEMENT_ROUNDS = 100


def _uniform_ts(rng: np.random.Generator, start: pd.Timestamp, end: pd.Timestamp, n: int) -> np.ndarray:
    """Return n UTC instants, whole seconds, uniform in [start, end), as int64 nanoseconds."""
    seconds = rng.integers(0, int((end - start).total_seconds()), n)
    return start.value + seconds * 1_000_000_000


def _to_utc(ns: np.ndarray) -> pd.DatetimeIndex:
    """Convert int64 nanoseconds to a UTC DatetimeIndex."""
    return pd.to_datetime(ns, utc=True)


def destination(lat, lon, km, bearing_rad) -> tuple[np.ndarray, np.ndarray]:
    """Return the point km away from (lat, lon) along the bearing, on a sphere (degrees in and out)."""
    lat1, lon1 = np.radians(lat), np.radians(lon)
    angle = np.asarray(km) / EARTH_RADIUS_KM
    lat2 = np.arcsin(np.sin(lat1) * np.cos(angle) + np.cos(lat1) * np.sin(angle) * np.cos(bearing_rad))
    lon2 = lon1 + np.arctan2(
        np.sin(bearing_rad) * np.sin(angle) * np.cos(lat1), np.cos(angle) - np.sin(lat1) * np.sin(lat2)
    )
    return np.degrees(lat2), np.degrees(lon2)


def build_hubs() -> pd.DataFrame:
    """Return the hubs table rows (the order weights stay in the generator, not in the OMS)."""
    hubs = hubs_frame().drop(columns="weight")
    hubs["created_ts"] = hubs["updated_ts"] = HUBS_OPENED
    return hubs


def build_couriers(n: int, seed: pd.DataFrame, rng: np.random.Generator) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (couriers, courier_changes) for n couriers.

    Each courier is hired with a home hub (by hub weight), a vehicle (LA mix), an employment type
    (by vehicle), a rating and an age (from the seed). During the period, rating reviews, hub
    transfers, vehicle changes and employment changes each append a full snapshot row to the audit.
    """
    hubs = hubs_frame()
    hub_ids, weights = hubs.hub_id.to_numpy(), hubs.weight.to_numpy()
    vehicles, vehicle_p = list(VEHICLE_MIX), np.array(list(VEHICLE_MIX.values()))
    steps, step_p = list(RATING_STEPS), list(RATING_STEPS.values())

    vehicle = rng.choice(vehicles, n, p=vehicle_p)
    employee = rng.random(n) < pd.Series(vehicle).map(EMPLOYEE_SHARE).to_numpy()
    state = pd.DataFrame(
        {
            "courier_id": np.arange(1, n + 1),
            "home_hub_id": rng.choice(hub_ids, n, p=weights),
            "vehicle_type": vehicle,
            "rating": rng.choice(seed.rating.to_numpy(), n),
            "employment_type": np.where(employee, "employee", "contractor"),
            "age": rng.choice(seed.age[seed.age >= MIN_COURIER_AGE].to_numpy(), n),
        }
    )
    during = rng.random(n) < HIRED_DURING_PERIOD
    hired = np.where(
        during,
        _uniform_ts(rng, PERIOD_START, PERIOD_END - LAST_HIRE_BEFORE_END, n),
        _uniform_ts(rng, COURIER_HIRE_FROM, PERIOD_START, n),
    )
    state["hired_ts"] = _to_utc(hired)

    period_ns = PERIOD_END.value - PERIOD_START.value
    changes = []
    for row, hired_ns in zip(state.itertuples(index=False), hired):
        current = {col: getattr(row, col) for col in TRACKED}
        changes.append((row.courier_id, hired_ns, "hired", *current.values()))
        start = max(hired_ns, PERIOD_START.value)
        employed_share = (PERIOD_END.value - start) / period_ns
        events = ["rating_review"] * rng.poisson(RATING_REVIEWS_PER_PERIOD * employed_share)
        events += ["hub_transfer"] * int(rng.random() < TRANSFER_SHARE)
        events += ["vehicle_change"] * int(rng.random() < VEHICLE_CHANGE_SHARE)
        events += ["employment_change"] * int(rng.random() < EMPLOYMENT_CHANGE_SHARE)
        times = start + 1_000_000_000 + rng.integers(0, (PERIOD_END.value - start) // 1_000_000_000 - 1, len(events)) * 1_000_000_000
        for reason, ts in sorted(zip(events, times), key=lambda e: e[1]):
            if reason == "rating_review":
                current["rating"] = float(np.clip(round(current["rating"] + rng.choice(steps, p=step_p), 1), 1.0, 5.0))
            elif reason == "hub_transfer":
                others = hub_ids != current["home_hub_id"]
                current["home_hub_id"] = rng.choice(hub_ids[others], p=weights[others] / weights[others].sum())
            elif reason == "vehicle_change":
                others = np.array(vehicles) != current["vehicle_type"]
                current["vehicle_type"] = rng.choice(np.array(vehicles)[others], p=vehicle_p[others] / vehicle_p[others].sum())
            else:
                current["employment_type"] = "contractor" if current["employment_type"] == "employee" else "employee"
            changes.append((row.courier_id, ts, reason, *current.values()))

    audit = pd.DataFrame(changes, columns=["courier_id", "changed_ts", "change_reason", *TRACKED])
    audit = audit.sort_values(["changed_ts", "courier_id"], kind="stable").reset_index(drop=True)
    audit.insert(0, "change_id", np.arange(1, len(audit) + 1))
    audit["changed_ts"] = _to_utc(audit.changed_ts.to_numpy())
    audit["rating"] = audit.rating.astype(float).round(1)

    latest = audit.groupby("courier_id").tail(1).set_index("courier_id")
    couriers = state.drop(columns=TRACKED).set_index("courier_id")
    couriers = latest[TRACKED].join(couriers).assign(updated_ts=latest.changed_ts).reset_index()
    couriers = couriers[["courier_id", *TRACKED, "age", "hired_ts", "updated_ts"]].sort_values("courier_id")
    return couriers.reset_index(drop=True), audit


def noop_changes(audit: pd.DataFrame) -> pd.Series:
    """Return True for each audit row whose tracked attributes equal the courier's previous row."""
    ordered = audit.sort_values(["courier_id", "changed_ts", "change_id"])
    previous = ordered.groupby("courier_id")[TRACKED].shift()
    same = (ordered[TRACKED] == previous).all(axis=1)
    return same.reindex(audit.index)


def build_customers(n: int, seed: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """Return n customers, each placed a seed distance from its service hub, inside the service area."""
    hubs = hubs_frame()
    pick = rng.choice(len(hubs), n, p=hubs.weight.to_numpy())
    hub_lat, hub_lon = hubs.lat.to_numpy()[pick], hubs.lon.to_numpy()[pick]
    home_lat, home_lon = np.empty(n), np.empty(n)
    todo = np.arange(n)
    for _ in range(MAX_PLACEMENT_ROUNDS):
        km = rng.choice(seed.km.to_numpy(), len(todo))
        lat, lon = destination(hub_lat[todo], hub_lon[todo], km, rng.uniform(0, 2 * np.pi, len(todo)))
        ok = inside_service_area(lat, lon)
        home_lat[todo[ok]], home_lon[todo[ok]] = lat[ok], lon[ok]
        todo = todo[~ok]
        if len(todo) == 0:
            break
    if len(todo):
        raise RuntimeError(f"{len(todo)} customers could not be placed inside the service area")

    during = rng.random(n) < SIGNED_UP_DURING_PERIOD
    signup = _to_utc(
        np.where(
            during,
            _uniform_ts(rng, PERIOD_START, PERIOD_END, n),
            _uniform_ts(rng, CUSTOMER_SIGNUP_FROM, PERIOD_START, n),
        )
    )
    return pd.DataFrame(
        {
            "customer_id": np.arange(1, n + 1),
            "service_hub_id": hubs.hub_id.to_numpy()[pick],
            "home_lat": home_lat.round(6),
            "home_lon": home_lon.round(6),
            "segment": rng.choice(list(SEGMENT_MIX), n, p=list(SEGMENT_MIX.values())),
            "signup_ts": signup,
            "updated_ts": signup,
        }
    )


def build_reference(scale: float, seed_value: int = DEFAULT_SEED) -> dict[str, pd.DataFrame]:
    """Return the four reference tables for the given scale (1.0 = 20k couriers, 5M customers)."""
    seed = load_seed()
    couriers, audit = build_couriers(max(1, round(FULL_COURIERS * scale)), seed, np.random.default_rng([seed_value, 1]))
    customers = build_customers(max(1, round(FULL_CUSTOMERS * scale)), seed, np.random.default_rng([seed_value, 2]))
    return {"hubs": build_hubs(), "couriers": couriers, "courier_changes": audit, "customers": customers}


def load(frames: dict[str, pd.DataFrame], replace: bool) -> None:
    """Create the tables if needed and load the frames in one transaction.

    Refuses to touch tables that already hold rows unless replace is True. Replacing also empties
    orders: they reference the hubs, couriers and customers being replaced, and Postgres refuses to
    truncate a referenced table on its own.
    """
    with db.connect("oms") as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_SQL.read_text())
        cur.execute("SELECT (SELECT count(*) FROM hubs) + (SELECT count(*) FROM couriers) + (SELECT count(*) FROM customers)")
        if cur.fetchone()[0] and not replace:
            raise SystemExit("oms already holds reference data; run again with --replace to overwrite it")
        cur.execute("TRUNCATE orders, courier_changes, customers, couriers, hubs")
        for table in TABLES:
            db.copy_frame(cur, table, frames[table])


def main() -> None:
    """Build the reference tables and load them into oms."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--scale", type=float, default=0.01, help="1.0 = full scale; 0.01 = the dev slice")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--replace", action="store_true", help="overwrite reference rows already in oms")
    args = parser.parse_args()
    started = time.perf_counter()
    frames = build_reference(args.scale, args.seed)
    built = time.perf_counter()
    load(frames, args.replace)
    audit = frames["courier_changes"]
    print(", ".join(f"{t} {len(frames[t]):,}" for t in TABLES))
    print(f"audit: {noop_changes(audit).sum():,} no-op rows; max {audit.groupby('courier_id').size().max()} rows per courier")
    print(f"built in {built - started:.1f} s, loaded in {time.perf_counter() - built:.1f} s")


if __name__ == "__main__":
    main()
