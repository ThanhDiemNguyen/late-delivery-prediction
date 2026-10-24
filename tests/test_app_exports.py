"""Checks on the courier-app exports: ping shape, idle trips, scans, and byte-identical files."""

import numpy as np
import pytest

from generator.app_exports import (
    COURIERS_PER_FILE,
    PING_SCHEMA,
    PINGS_PER_SHIFT,
    build_ping_file,
    build_scans,
    parquet_bytes,
)
from generator.effect_model import haversine_km
from generator.la_hubs import hubs_frame, inside_validation_box
from generator.oms_orders import build_orders, courier_state_at, local_days
from generator.oms_reference import build_reference


@pytest.fixture(scope="module")
def first_day():
    """The first day's orders, courier state and trips at 1% scale."""
    reference = build_reference(0.01)
    orders = next(build_orders(0.01, reference, days=1))
    day = local_days()[0]
    state = courier_state_at(reference["courier_changes"], reference["couriers"], day.tz_convert("UTC"))
    couriers = state.sort_values("courier_id").head(COURIERS_PER_FILE)[["courier_id", "home_hub_id", "vehicle_type"]]
    trips = {int(c): g.order_id.to_numpy() for c, g in orders.sort_values("pickup_ts").groupby("courier_id")}
    return day, orders, couriers, trips


@pytest.fixture(scope="module")
def pings(first_day):
    """One ping file for the first day."""
    day, _, couriers, trips = first_day
    return build_ping_file(day, 0, couriers, trips, np.random.default_rng([1, 4, 0, 0]))


def test_ping_file_shape(first_day, pings):
    """Every courier sends 1,440 pings 25 s apart, in the v1 schema, during a 10-hour local shift."""
    _, _, couriers, _ = first_day
    assert pings.schema == PING_SCHEMA
    frame = pings.to_pandas()
    assert len(frame) == len(couriers) * PINGS_PER_SHIFT
    gaps = frame.groupby("courier_id").ping_ts.diff().dropna().dt.total_seconds()
    assert (gaps == 25).all()
    local_hours = frame.ping_ts.dt.tz_convert("America/Los_Angeles").dt.hour
    assert local_hours.between(7, 23).all()
    assert (frame.uploaded_ts > frame.ping_ts).all()


def test_idle_pings_have_no_trip_and_trips_are_orders(first_day, pings):
    """About 30% of pings are idle (NULL trip_id); every trip_id is one of that courier's orders."""
    _, orders, _, _ = first_day
    frame = pings.to_pandas()
    assert frame.trip_id.isna().mean() == pytest.approx(0.30, abs=0.03)
    own = orders.set_index("order_id").courier_id
    with_trip = frame.dropna(subset=["trip_id"])
    assert (own.loc[with_trip.trip_id.astype(int)].to_numpy() == with_trip.courier_id.to_numpy()).all()
    assert with_trip.speed_kmh.mean() > 10 * frame[frame.trip_id.isna()].speed_kmh.mean()


def test_pings_stay_near_their_hub_in_la(pings):
    """Couriers stay within about 20 km of their hub, inside the LA validation box."""
    frame = pings.to_pandas()
    hub = hubs_frame().set_index("hub_id").loc[frame.hub_id]
    km = haversine_km(hub.lat.to_numpy(), hub.lon.to_numpy(), frame.lat, frame.lon)
    assert np.median(km) < 8 and km.max() < 25
    assert inside_validation_box(frame.lat, frame.lon).mean() > 0.99


def test_three_scans_per_order_in_lifecycle_order(first_day):
    """at_hub, then picked_up, then delivered; the at_hub scan has no courier."""
    _, orders, _, _ = first_day
    scans = build_scans(orders, np.random.default_rng(5))
    assert len(scans) == 3 * len(orders) and scans.scan_id.is_unique
    times = scans.scan_ns.to_numpy().reshape(-1, 3)
    assert (times[:, 0] < times[:, 1]).all() and (times[:, 1] < times[:, 2]).all()
    assert scans.courier_id[scans.scan_type == "at_hub"].isna().all()
    assert scans.courier_id[scans.scan_type != "at_hub"].notna().all()


def test_same_seed_gives_byte_identical_files(first_day):
    """A file rebuilt with the same seed has the same bytes, so a rerun has the same hashes."""
    day, _, couriers, trips = first_day
    first = parquet_bytes(build_ping_file(day, 0, couriers, trips, np.random.default_rng([1, 4, 0, 0])))
    again = parquet_bytes(build_ping_file(day, 0, couriers, trips, np.random.default_rng([1, 4, 0, 0])))
    other = parquet_bytes(build_ping_file(day, 0, couriers, trips, np.random.default_rng([1, 4, 0, 1])))
    assert first == again and first != other
