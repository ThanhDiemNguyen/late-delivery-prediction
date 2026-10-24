"""Checks on the live scan events: exact late and retry shares, delays, order, and the stream checks."""

import numpy as np
import pandas as pd
import pytest
from confluent_kafka import TIMESTAMP_CREATE_TIME

from generator.check_stream import measure
from generator.scan_producer import (
    DUPLICATE_SHARE, EVENT_COLUMNS, LATE_DELAY_MS, LATE_SHARE, LATE_SHARE_WEAK_SIGNAL, ON_TIME_DELAY_MS, RETRY_GAP_MS,
    build_events,
)

N_ORDERS = 20_000
AREAS = ["Metropolitian", "Urban", "Semi-Urban", "Other"]


@pytest.fixture(scope="module")
def source():
    """Synthetic scans (three per order, hub scans without a courier) and their orders."""
    rng = np.random.default_rng(7)
    order_id = np.arange(1, N_ORDERS + 1)
    start = int(pd.Timestamp("2024-11-04", tz="UTC").timestamp() * 1000)
    order_ts = start + np.sort(rng.integers(0, 7 * 86_400_000, N_ORDERS))
    scans = pd.DataFrame({
        "scan_id": np.repeat(order_id * 3 - 2, 3) + np.tile(np.arange(3), N_ORDERS),
        "order_id": np.repeat(order_id, 3),
        "courier_id": pd.array(np.where(np.tile([True, False, False], N_ORDERS), pd.NA, np.repeat(order_id % 500, 3)), dtype="Int32"),
        "hub_id": np.repeat(order_id % 40, 3).astype(np.int16),
        "scan_type": np.tile(["at_hub", "picked_up", "delivered"], N_ORDERS),
        "scan_ts": np.repeat(order_ts, 3) + np.tile([60_000, 600_000, 2_400_000], N_ORDERS),
    })
    orders = pd.DataFrame({
        "order_id": order_id,
        "promised_ts": order_ts + 2_700_000,
        "area": rng.choice(AREAS, N_ORDERS, p=[0.55, 0.35, 0.07, 0.03]),
    })
    return scans, orders


@pytest.fixture(scope="module")
def events(source):
    """The events built from the synthetic source with a fixed seed."""
    scans, orders = source
    return build_events(scans, orders, np.random.default_rng([1, 30]))


def first_sends(events):
    """Return the first send of each scan and its delay."""
    first = events.drop_duplicates("scan_id").copy()
    first["delay"] = first.sent_ts - first.scan_ts
    return first


def test_columns_and_arrival_order(events):
    """Events carry the contract columns and go out in arrival order."""
    assert list(events.columns) == EVENT_COLUMNS
    assert events.sent_ts.is_monotonic_increasing


def test_late_shares_are_exact(source, events):
    """Exactly 8% of scans are late overall and 25% of the weak-signal ones."""
    scans, orders = source
    first = first_sends(events).merge(orders[["order_id", "area"]], on="order_id")
    late = first.delay >= LATE_DELAY_MS[0]
    weak = first.area.isin(["Semi-Urban", "Other"])
    assert late.sum() == round(LATE_SHARE * len(scans))
    assert late[weak].sum() == round(LATE_SHARE_WEAK_SIGNAL * weak.sum())
    assert late[~weak].mean() < LATE_SHARE < late[weak].mean()


def test_delays_stay_in_their_ranges(events):
    """Late scans arrive 2 to 20 minutes after the event, the others within 3 s."""
    first = first_sends(events)
    late = first.delay >= LATE_DELAY_MS[0]
    assert first.delay[late].between(*LATE_DELAY_MS).all()
    assert first.delay[~late].between(*ON_TIME_DELAY_MS).all()


def test_retries(source, events):
    """1.50% of scans are sent again 1 to 10 s later with the same payload."""
    scans, _ = source
    assert len(events) - len(scans) == round(DUPLICATE_SHARE * len(scans))
    retried = events[events.duplicated("scan_id", keep=False)]
    gaps = retried.groupby("scan_id").sent_ts.agg(lambda s: s.max() - s.min())
    assert gaps.between(*RETRY_GAP_MS).all()
    assert retried.drop(columns="sent_ts").drop_duplicates().scan_id.is_unique


def test_same_seed_same_events(source, events):
    """A rerun with the same seed builds identical events."""
    scans, orders = source
    pd.testing.assert_frame_equal(events, build_events(scans, orders, np.random.default_rng([1, 30])))


def test_missing_order_is_refused(source):
    """A scan whose order is not in oms stops the build."""
    scans, orders = source
    with pytest.raises(ValueError, match="no order in oms"):
        build_events(scans, orders.iloc[1:], np.random.default_rng(0))


def test_stream_checks_pass_on_the_events(source, events):
    """check_stream's checks all pass on the events as one partition would hold them."""
    scans, orders = source
    records = events.assign(partition=0, offset=np.arange(len(events)), ts_type=TIMESTAMP_CREATE_TIME,
                            record_ts=events.scan_ts)
    records["courier_id"] = records.courier_id.astype("float")
    checks, facts = measure(records, orders[["order_id", "area"]], scans)
    assert [c.name for c in checks if not c.passed] == []
    assert facts["records behind the newest event time %"] > 0
