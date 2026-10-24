"""Checks on the courier-app exports: ping shape, trips, v2 rollout, duplicates, scans, identical files."""

import numpy as np
import pandas as pd
import pytest

from generator.app_exports import (
    COURIERS_PER_FILE,
    DUPLICATE_SHARE,
    PING_SCHEMA,
    PING_SCHEMA_V2,
    PINGS_PER_SHIFT,
    build_ping_files,
    build_scans,
    parquet_bytes,
    runs_v2,
)
from generator.effect_model import haversine_km
from generator.la_config import TIME_ZONE
from generator.la_hubs import hubs_frame, inside_validation_box
from generator.oms_orders import build_orders, courier_state_at, local_days
from generator.oms_reference import build_reference

KEY = ["courier_id", "device_seq"]


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


def _files(first_day, day=None, part=0):
    """Build one courier group's ping files for the first day (or another day) as {suffix: DataFrame}."""
    first, _, couriers, trips = first_day
    files = build_ping_files(day or first, 0, couriers, trips, np.random.default_rng([1, 4, 0, part]))
    return {suffix: table for suffix, table in files}


@pytest.fixture(scope="module")
def pings(first_day):
    """The first day's single v1 ping file, as a DataFrame."""
    files = _files(first_day)
    assert list(files) == [""]
    return files[""].to_pandas()


def test_ping_file_shape(first_day, pings):
    """Every courier sends 1,440 pings 25 s apart, in the v1 schema, during a 10-hour local shift."""
    _, _, couriers, _ = first_day
    assert _files(first_day)[""].schema == PING_SCHEMA
    originals = pings.drop_duplicates(KEY).sort_values(KEY)
    assert len(originals) == len(couriers) * PINGS_PER_SHIFT
    gaps = originals.groupby("courier_id").ping_ts.diff().dropna().dt.total_seconds()
    assert (gaps == 25).all()
    assert originals.ping_ts.dt.tz_convert(TIME_ZONE).dt.hour.between(7, 23).all()
    assert (pings.uploaded_ts > pings.ping_ts).all()


def test_idle_pings_have_no_trip_and_trips_are_orders(first_day, pings):
    """About 30% of pings are idle (NULL trip_id); every trip_id is one of that courier's orders."""
    _, orders, _, _ = first_day
    assert pings.trip_id.isna().mean() == pytest.approx(0.30, abs=0.03)
    own = orders.set_index("order_id").courier_id
    with_trip = pings.dropna(subset=["trip_id"])
    assert (own.loc[with_trip.trip_id.astype(int)].to_numpy() == with_trip.courier_id.to_numpy()).all()
    assert with_trip.speed_kmh.mean() > 10 * pings[pings.trip_id.isna()].speed_kmh.mean()


def test_pings_stay_near_their_hub_in_la(pings):
    """Couriers stay within about 20 km of their hub, inside the LA validation box."""
    hub = hubs_frame().set_index("hub_id").loc[pings.hub_id]
    km = haversine_km(hub.lat.to_numpy(), hub.lon.to_numpy(), pings.lat, pings.lon)
    assert np.median(km) < 8 and km.max() < 25
    assert inside_validation_box(pings.lat, pings.lon).mean() > 0.99


def test_duplicates_are_reuploaded_batches(first_day, pings):
    """2.00% extra rows: whole 5-minute batches with the same key and content and a later uploaded_ts."""
    _, _, couriers, _ = first_day
    originals = len(couriers) * PINGS_PER_SHIFT
    assert len(pings) - originals == round(DUPLICATE_SHARE * originals / 12) * 12
    copies = pings[pings.duplicated(KEY, keep="first")]
    firsts = pings.drop_duplicates(KEY).set_index(KEY).loc[pd.MultiIndex.from_frame(copies[KEY])]
    same = copies.drop(columns=["uploaded_ts"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(same, firsts.reset_index()[same.columns].reset_index(drop=True))
    assert (copies.uploaded_ts.to_numpy() > firsts.uploaded_ts.to_numpy()).all()
    assert (copies.groupby("courier_id").size() % 12 == 0).all()


def test_v2_rolls_out_over_14_days_from_september():
    """No courier runs v2 before 2024-09-01, about 1/14 more each day, and all of them from 09-14."""
    ids = np.arange(1, 20_001)
    share = {d: runs_v2(pd.Timestamp(d, tz=TIME_ZONE), ids).mean() for d in ("2024-08-31", "2024-09-01", "2024-09-07", "2024-09-14")}
    assert share["2024-08-31"] == 0
    assert share["2024-09-01"] == pytest.approx(1 / 14, abs=0.005)
    assert share["2024-09-07"] == pytest.approx(7 / 14, abs=0.005)
    assert share["2024-09-14"] == 1


def test_rollout_day_writes_both_schemas(first_day):
    """On a rollout day, v1 and v2 couriers go to separate files; v2 swaps speed_kmh for speed_mps."""
    files = _files(first_day, day=pd.Timestamp("2024-09-07", tz=TIME_ZONE))
    assert set(files) == {"", "-v2"}
    assert files[""].schema == PING_SCHEMA and files["-v2"].schema == PING_SCHEMA_V2
    v2 = files["-v2"].to_pandas()
    assert "speed_kmh" not in v2 and (v2.app_version == "4.0.0").all()
    assert v2.battery_pct.between(5, 100).all() and v2.gps_accuracy_m.between(0.5, 60).all()
    v1_ids, v2_ids = set(files[""].column("courier_id").to_pylist()), set(v2.courier_id)
    assert not v1_ids & v2_ids
    later = _files(first_day, day=pd.Timestamp("2024-09-20", tz=TIME_ZONE))
    assert set(later) == {"-v2"}


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
    first = parquet_bytes(_files(first_day)[""])
    again = parquet_bytes(_files(first_day)[""])
    other = parquet_bytes(_files(first_day, part=1)[""])
    assert first == again and first != other
