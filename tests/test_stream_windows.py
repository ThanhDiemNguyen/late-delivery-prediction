"""Checks on the window truth, the simulated drops and the comparison used to judge the Flink runs."""

import pandas as pd

from generator.stream_windows import COUNTS, WINDOW_MS, compare, final_windows, simulated_drops, truth_windows

MIN = 60_000


def events(*rows):
    """Return events in arrival order from (scan_id, scan_ts) pairs."""
    return pd.DataFrame(rows, columns=["scan_id", "scan_ts"])


def test_truth_counts_scans_delivered_and_late_delivered():
    """Each hub and window counts its scans, its delivered scans and those after promised_ts."""
    scans = pd.DataFrame({
        "order_id": [1, 1, 2, 2],
        "hub_id": [7, 7, 7, 7],
        "scan_type": ["picked_up", "delivered", "picked_up", "delivered"],
        "scan_ts": [1 * MIN, 2 * MIN, 3 * MIN, 6 * MIN],
    })
    orders = pd.DataFrame({"order_id": [1, 2], "promised_ts": [1 * MIN, 10 * MIN]})
    truth = truth_windows(scans, orders).set_index("window_start")
    assert truth.loc[0, COUNTS].tolist() == [3, 1, 1]
    assert truth.loc[WINDOW_MS, COUNTS].tolist() == [1, 1, 0]


def test_a_scan_behind_a_closed_window_is_dropped_only_without_lateness():
    """A scan from window [0, 5) arriving after event time 5 min + 10 s is dropped, unless lateness covers it."""
    stream = events((1, 1 * MIN), (2, 6 * MIN), (3, 2 * MIN))
    assert simulated_drops(stream, lateness_ms=0, dedup=False) == 1
    assert simulated_drops(stream, lateness_ms=20 * MIN, dedup=False) == 0


def test_a_scan_within_the_bound_is_kept():
    """A scan less than 10 s behind the newest event time still finds its window open."""
    stream = events((1, WINDOW_MS - 1), (2, WINDOW_MS + 5_000), (3, WINDOW_MS - 2))
    assert simulated_drops(stream, lateness_ms=0, dedup=False) == 0


def test_dedup_counts_a_late_retry_once():
    """With dedup, a retry is removed before the window, so its late copy is not dropped again."""
    stream = events((1, 1 * MIN), (2, 6 * MIN), (3, 2 * MIN), (3, 2 * MIN))
    assert simulated_drops(stream, lateness_ms=0, dedup=False) == 2
    assert simulated_drops(stream, lateness_ms=0, dedup=True) == 1


def test_final_window_is_the_last_firing():
    """A window that fired again after a late scan is judged by its last firing."""
    firings = pd.DataFrame({
        "hub_id": [7, 7, 8], "window_start": [0, 0, 0], "watermark": [100, 200, 100],
        "scans": [3, 4, 1], "delivered": [1, 2, 0], "late_delivered": [0, 1, 0],
    })
    final = final_windows(firings).set_index("hub_id")
    assert final.loc[7, COUNTS].tolist() == [4, 2, 1]


def test_compare_classifies_windows():
    """Windows are exact, over- or under-counted, missing from the output or extra in it."""
    key = ["hub_id", "window_start"]
    truth = pd.DataFrame([(1, 0, 3, 1, 0), (2, 0, 3, 1, 0), (3, 0, 3, 1, 0), (4, 0, 1, 0, 0)], columns=key + COUNTS)
    flink = pd.DataFrame([(1, 0, 3, 1, 0), (2, 0, 4, 1, 0), (3, 0, 2, 1, 0), (5, 0, 1, 0, 0)], columns=key + COUNTS)
    result = compare(truth, flink)
    assert (result["exact"], result["over-counted"], result["under-counted"]) == (1, 2, 2)
    assert (result["missing"], result["extra"], result["truth windows"]) == (1, 1, 4)
