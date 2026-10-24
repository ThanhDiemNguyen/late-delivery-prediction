"""Checks on the OMS reference data: hub config, couriers and their audit, customers, determinism."""

import numpy as np
import pandas as pd
import pytest

from generator.effect_model import haversine_km
from generator.la_config import AREA_MIX, PERIOD_END, VEHICLE_MIX
from generator.la_hubs import hubs_frame, inside_service_area, inside_validation_box
from generator.oms_reference import TRACKED, build_reference, noop_changes


@pytest.fixture(scope="module")
def frames():
    """Reference tables at 5% scale: 1,000 couriers and 250,000 customers."""
    return build_reference(0.05)


def test_hub_weights_carry_the_skew_and_the_area_mix():
    """Weights sum to 1, Downtown LA has 25%, the top 10 have 60%, and each area type matches AREA_MIX."""
    hubs = hubs_frame()
    assert len(hubs) == 40 and hubs.hub_id.is_unique and hubs.name.is_unique
    assert hubs.weight.sum() == pytest.approx(1.0, abs=1e-9)
    top = hubs.sort_values("weight", ascending=False)
    assert top.iloc[0]["name"] == "Downtown LA" and top.iloc[0].weight == pytest.approx(0.25)
    assert top.weight.head(10).sum() == pytest.approx(0.60, abs=1e-9)
    by_area = hubs.groupby("area_type").weight.sum()
    for area, share in AREA_MIX.items():
        assert by_area[area] == pytest.approx(share, abs=1e-9), area


def test_every_hub_is_inside_the_service_area_and_the_box():
    """No hub sits in the ocean or outside the validation box."""
    hubs = hubs_frame()
    assert inside_service_area(hubs.lat, hubs.lon).all()
    assert inside_validation_box(hubs.lat, hubs.lon).all()


def test_service_polygon_on_known_points():
    """Downtown and Pomona are inside; two points in the Pacific are outside."""
    lat = [34.05, 34.055, 34.00, 33.90]
    lon = [-118.25, -117.75, -118.55, -118.60]
    assert inside_service_area(lat, lon).tolist() == [True, True, False, False]


def test_couriers_current_state_is_their_last_audit_row(frames):
    """The couriers table equals each courier's latest courier_changes row."""
    couriers, audit = frames["couriers"], frames["courier_changes"]
    latest = audit.sort_values("change_id").groupby("courier_id").tail(1).set_index("courier_id")
    current = couriers.set_index("courier_id")
    pd.testing.assert_frame_equal(current[TRACKED], latest[TRACKED].loc[current.index])
    assert (current.updated_ts == latest.changed_ts.loc[current.index]).all()


def test_audit_rows_are_ordered_full_snapshots(frames):
    """change_id follows time, every courier starts with a hire, and changes stay before the period end."""
    audit = frames["courier_changes"]
    assert audit.change_id.is_unique and audit.changed_ts.is_monotonic_increasing
    first = audit.sort_values("change_id").groupby("courier_id").head(1)
    assert (first.change_reason == "hired").all() and len(first) == len(frames["couriers"])
    assert (audit.changed_ts < PERIOD_END).all()
    assert audit[TRACKED].notna().all().all()


def test_audit_has_real_changes_and_noops(frames):
    """Some couriers change several times, and some reviews change nothing (SCD2 must skip them)."""
    audit = frames["courier_changes"]
    assert audit.groupby("courier_id").size().max() >= 3
    noops = noop_changes(audit)
    assert 0.15 < noops.mean() < 0.40
    assert (audit.change_reason[noops] == "rating_review").all()
    assert 5.0 < len(audit) / len(frames["couriers"]) < 6.5


def test_courier_attributes_follow_the_config(frames):
    """Vehicle shares follow the LA mix; ages are adults; ratings stay in 1 to 5 with one decimal."""
    couriers = frames["couriers"]
    shares = couriers.vehicle_type.value_counts(normalize=True)
    for vehicle, share in VEHICLE_MIX.items():
        assert shares[vehicle] == pytest.approx(share, abs=0.04), vehicle
    assert couriers.age.min() >= 18
    assert couriers.rating.between(1, 5).all()
    assert (couriers.rating * 10 == (couriers.rating * 10).round()).all()


def test_customers_are_inside_la_at_seed_distances(frames):
    """Every home is inside the service area, about 9.2 km (the seed median) from its hub, with the hub skew."""
    customers, hubs = frames["customers"], hubs_frame().set_index("hub_id")
    assert inside_service_area(customers.home_lat, customers.home_lon).all()
    hub = hubs.loc[customers.service_hub_id]
    km = haversine_km(hub.lat.to_numpy(), hub.lon.to_numpy(), customers.home_lat, customers.home_lon)
    assert np.median(km) == pytest.approx(9.2, abs=0.3)
    assert (customers.service_hub_id == 1).mean() == pytest.approx(0.25, abs=0.005)


def test_same_seed_gives_identical_tables():
    """A rebuild with the same scale and seed is identical; another seed is not."""
    first, second, other = build_reference(0.002), build_reference(0.002), build_reference(0.002, seed_value=7)
    for table in first:
        assert first[table].equals(second[table]), table
    assert not first["customers"].equals(other["customers"])
