"""Checks that generated orders carry the label rate, the skew and the seed's dirty values."""

import numpy as np
import pandas as pd
import pytest

from generator.effect_model import SEED_RENAMES
from generator.la_config import AREA_MIX, TIME_ZONE
from generator.la_hubs import hubs_frame
from generator.oms_orders import (
    LEGACY_AREA,
    LEGACY_TRAFFIC,
    LEGACY_VEHICLE,
    build_orders,
    orders_per_day,
    seed_patterns,
)
from generator.oms_reference import build_reference

DAYS = 14


@pytest.fixture(scope="module")
def orders():
    """Two weeks of orders at 5% scale (20,000 a day)."""
    return pd.concat(build_orders(0.05, build_reference(0.05), days=DAYS), ignore_index=True)


@pytest.fixture(scope="module")
def patterns():
    """The dirty-value rates measured in the seed."""
    return seed_patterns()


def _clean(orders: pd.DataFrame) -> pd.DataFrame:
    """Undo the legacy spellings, as the silver layer will."""
    return orders.assign(
        traffic=orders.traffic.str.strip(),
        vehicle=orders.vehicle.str.strip(),
        area=orders.area.str.strip().replace(SEED_RENAMES["area"]),
    )


def test_daily_counts_include_the_peak_weeks():
    """400k orders a day at full scale, 2x in the July sale week and 3x in Black Friday week."""
    counts = orders_per_day(1.0)
    assert len(counts) == 183
    assert counts[pd.Timestamp("2024-06-01", tz=TIME_ZONE)] == 400_000
    assert counts[pd.Timestamp("2024-07-16", tz=TIME_ZONE)] == 800_000
    assert counts[pd.Timestamp("2024-11-29", tz=TIME_ZONE)] == 1_200_000
    assert counts.sum() == 80_800_000


def test_order_ids_follow_time_and_deliveries_follow_orders(orders):
    """order_id runs 1..n in order time, and every delivery is at least 10 minutes after its order."""
    assert orders.order_id.tolist() == list(range(1, len(orders) + 1))
    timed = orders[orders.order_time != "NaN "]
    ordered = pd.to_datetime(timed.order_date.astype(str) + " " + timed.order_time).dt.tz_localize(TIME_ZONE)
    assert ordered.is_monotonic_increasing
    assert ((timed.delivered_ts - ordered.dt.tz_convert("UTC")) >= pd.Timedelta(minutes=10)).all()
    assert (orders.updated_ts == orders.delivered_ts).all()


def test_late_share_matches_the_calibration(orders):
    """About 17% of orders are late, as the LA SLA was fitted for."""
    late = (orders.delivered_ts > orders.promised_ts).mean()
    assert late == pytest.approx(0.17, abs=0.01)


def test_hub_skew_and_area_mix(orders):
    """Downtown LA has 25% of orders, the top 10 hubs 60%, and the areas follow the LA mix."""
    share = orders.hub_id.value_counts(normalize=True)
    assert share[1] == pytest.approx(0.25, abs=0.01)
    assert share.head(10).sum() == pytest.approx(0.60, abs=0.015)
    areas = _clean(orders).area.value_counts(normalize=True)
    for area, mix in AREA_MIX.items():
        assert areas[area] == pytest.approx(mix, abs=0.01), area


def test_legacy_spellings_match_the_seed(orders):
    """Traffic, vehicle and area use only the legacy spellings, plus the NaN literal."""
    assert set(orders.traffic) <= set(LEGACY_TRAFFIC.values()) | {"NaN "}
    assert set(orders.vehicle) <= set(LEGACY_VEHICLE.values())
    assert set(orders.area) == set(LEGACY_AREA.values())
    assert "Metropolitian " in set(orders.area)


def test_null_block_rate_matches_the_seed(orders, patterns):
    """The same rows hold NaN traffic, weather and order_time, at the seed's 0.21%."""
    block = orders.order_time == "NaN "
    assert block.mean() == pytest.approx(patterns.null_block_share, abs=0.0005)
    assert (orders.traffic[block] == "NaN ").all() and (orders.weather[block] == "NaN").all()


def test_store_coordinates_break_at_the_seed_rates(orders, patterns):
    """(0, 0) at 8.01%, a flipped latitude at 0.43%; every other store point is its hub."""
    zero = (orders.store_lat == 0) & (orders.store_lon == 0)
    flipped = orders.store_lat < 0
    assert zero.mean() == pytest.approx(patterns.store_zero_share, abs=0.003)
    assert flipped.mean() == pytest.approx(patterns.lat_flip_share, abs=0.0008)
    hubs = hubs_frame().set_index("hub_id")
    good = orders[~zero & ~flipped]
    assert np.allclose(good.store_lat, hubs.lat.loc[good.hub_id].to_numpy())


def test_pickup_rollover_appears_at_about_the_seed_rate(orders):
    """Orders just before midnight are picked up after it, so pickup_time < order_time (seed: 1.89%)."""
    timed = orders[orders.order_time != "NaN "]
    rollover = (timed.pickup_time < timed.order_time).mean()
    assert rollover == pytest.approx(0.0189, abs=0.004)


def test_local_text_matches_the_instants(orders):
    """order_date and order_time are LA wall-clock time; promised_ts is order time plus the SLA."""
    timed = orders[orders.order_time != "NaN "].head(1000)
    local = pd.to_datetime(timed.order_date.astype(str) + " " + timed.order_time).dt.tz_localize(TIME_ZONE)
    sla_min = (timed.promised_ts - local.dt.tz_convert("UTC")).dt.total_seconds() / 60
    assert sla_min.between(120, 215 + 3 * 25).all()


def test_same_seed_gives_identical_orders():
    """A rebuild with the same scale and seed is identical."""
    reference = build_reference(0.002)
    first = pd.concat(build_orders(0.002, reference, days=2))
    second = pd.concat(build_orders(0.002, reference, days=2))
    assert first.equals(second)
