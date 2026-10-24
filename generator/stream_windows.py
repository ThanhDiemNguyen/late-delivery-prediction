"""Run the Flink scan-window job for one mode and check its windows against the truth.

    uv run python -m generator.stream_windows --mode baseline

1. Build the week's events as scan_producer does: the same seed gives the same events.
2. Empty parcel-scans-replay, hub-delivery-5m and scans-late.
3. Submit flink/jobs/scan_windows.py to the session cluster, bounded at the offset after the last
   event, and wait until its tasks run.
4. Send the events at --rate per second. Flink emits a watermark every millisecond, so at 1,000
   events/s the watermark moves about one event at a time, as it would on a live stream. A replay
   read at full speed would move it hours of event time at once and hide the late arrivals.
5. Wait until the job finishes: the source stops at the stop offset, and the final watermark fires
   every window.
6. Recompute the windows from the export Parquet (the truth) and compare them with the last firing
   of each output window. The dropped scans are compared with what a watermark after every record
   would drop.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
import urllib.request

import numpy as np
import pandas as pd
from confluent_kafka import Consumer, TopicPartition

from generator import db
from generator.check_data import Check
from generator.check_stream import read_topic
from generator.oms_reference import DEFAULT_SEED
from generator.scan_producer import BOOTSTRAP, build_events, read_orders, read_scans, send, truncate
from generator.storage import s3_client

SOURCE, OUTPUT, LATE = "parcel-scans-replay", "hub-delivery-5m", "scans-late"
FLINK_REST = "http://127.0.0.1:8084"
WINDOW_MS, OUT_OF_ORDERNESS_MS = 300_000, 10_000
ALLOWED_LATENESS_MS = {"baseline": 0, "lateness": 20 * 60_000, "dedup": 20 * 60_000}
DEDUP = {"baseline": False, "lateness": False, "dedup": True}
COUNTS = ["scans", "delivered", "late_delivered"]


def truth_windows(scans: pd.DataFrame, orders: pd.DataFrame) -> pd.DataFrame:
    """Return the counts of every hub and 5-minute window, from the distinct exported scans."""
    scans = scans.merge(orders[["order_id", "promised_ts"]], on="order_id")
    delivered = scans.scan_type == "delivered"
    return (
        scans.assign(window_start=scans.scan_ts // WINDOW_MS * WINDOW_MS, scans=1, delivered=delivered.astype(int),
                     late_delivered=(delivered & (scans.scan_ts > scans.promised_ts)).astype(int))
        .groupby(["hub_id", "window_start"], as_index=False)[COUNTS].sum()
    )


def simulated_drops(events: pd.DataFrame, lateness_ms: int, dedup: bool) -> int:
    """Return how many events a watermark updated after every record would drop, in arrival order.

    Flink drops an element when its window's last millisecond plus the allowed lateness is at or
    behind the watermark, which trails the newest event time seen by the bound plus 1 ms.
    """
    if dedup:
        events = events.drop_duplicates("scan_id")
    ts = events.scan_ts.to_numpy()
    newest_before = np.maximum.accumulate(np.concatenate([[np.iinfo(np.int64).min // 2], ts[:-1]]))
    watermark = newest_before - OUT_OF_ORDERNESS_MS - 1
    window_end = (ts // WINDOW_MS + 1) * WINDOW_MS
    return int((window_end - 1 + lateness_ms <= watermark).sum())


def final_windows(records: pd.DataFrame) -> pd.DataFrame:
    """Return the last firing of each output window: the highest watermark, then the highest count."""
    return (
        records.sort_values(["watermark", "scans"])
        .drop_duplicates(["hub_id", "window_start"], keep="last")[["hub_id", "window_start", *COUNTS]]
    )


def compare(truth: pd.DataFrame, output: pd.DataFrame) -> dict:
    """Return how many windows match the truth exactly, are over- or under-counted, missing or extra."""
    both = truth.merge(output, on=["hub_id", "window_start"], how="outer", suffixes=("_truth", "_flink"), indicator=True)
    t = both[[f"{c}_truth" for c in COUNTS]].fillna(0).to_numpy()
    f = both[[f"{c}_flink" for c in COUNTS]].fillna(0).to_numpy()
    in_truth = both._merge != "right_only"
    return {
        "truth windows": int(in_truth.sum()),
        "output windows": int((both._merge != "left_only").sum()),
        "exact": int((t == f).all(axis=1)[in_truth].sum()),
        "over-counted": int((f > t).any(axis=1).sum()),
        "under-counted": int((f < t).any(axis=1).sum()),
        "missing": int((both._merge == "left_only").sum()),
        "extra": int((both._merge == "right_only").sum()),
        "totals truth": dict(zip(COUNTS, t.sum(axis=0).astype(int).tolist())),
        "totals flink": dict(zip(COUNTS, f.sum(axis=0).astype(int).tolist())),
    }


def end_offset(topic: str) -> int:
    """Return the offset after the last record of partition 0."""
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "stream-windows"})
    high = consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=10)[1]
    consumer.close()
    return high


def rest(path: str) -> dict:
    """Return one Flink REST response."""
    with urllib.request.urlopen(f"{FLINK_REST}{path}", timeout=10) as response:
        return json.load(response)


def submit(mode: str, stop_offset: int) -> str:
    """Submit the job, detached, and return its id."""
    out = subprocess.run(
        ["docker", "compose", "exec", "-T", "flink-jobmanager", "flink", "run", "-d", "-py", "/opt/flink/jobs/scan_windows.py",
         "--mode", mode, "--stop-offset", str(stop_offset)],
        capture_output=True, text=True, check=False,
    )
    found = re.search(r"JobID ([0-9a-f]{32})", out.stdout)
    if not found:
        raise RuntimeError(f"the job was not submitted:\n{out.stdout[-2000:]}\n{out.stderr[-2000:]}")
    return found.group(1)


def wait_for(job_id: str, ready, timeout_s: int) -> dict:
    """Poll the job until ready(job) holds; stop on a failed or cancelled job."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        job = rest(f"/jobs/{job_id}")
        if job["state"] in ("FAILED", "CANCELED"):
            raise RuntimeError(f"job {job_id} is {job['state']}; see the Flink UI")
        if ready(job):
            return job
        time.sleep(1)
    raise TimeoutError(f"job {job_id} not ready after {timeout_s} s")


def main() -> None:
    """Run one mode end to end and print the comparison with the truth."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--mode", choices=sorted(ALLOWED_LATENESS_MS), required=True)
    parser.add_argument("--start", default="2024-11-04")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--rate", type=float, default=1000, help="events per second sent while the job runs")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    args = parser.parse_args()

    scans = read_scans(s3_client(), args.start, args.days)
    with db.connect("oms") as conn, conn.cursor() as cur:
        orders = read_orders(cur, scans.order_id.unique())
    events = build_events(scans, orders, np.random.default_rng([args.seed, 30]))
    for topic in (SOURCE, OUTPUT, LATE):
        truncate(topic, BOOTSTRAP)
    stop = end_offset(SOURCE) + len(events)
    job_id = submit(args.mode, stop)
    wait_for(job_id, lambda j: j["state"] == "RUNNING" and all(v["status"] == "RUNNING" for v in j["vertices"]), 120)
    sent_s = send(events, SOURCE, BOOTSTRAP, args.rate)
    job = wait_for(job_id, lambda j: j["state"] == "FINISHED", 600)

    output = read_topic(OUTPUT, BOOTSTRAP)
    dropped = len(read_topic(LATE, BOOTSTRAP))
    result = compare(truth_windows(scans, orders), final_windows(output))
    lateness, dedup = ALLOWED_LATENESS_MS[args.mode], DEDUP[args.mode]
    read = len(events.drop_duplicates("scan_id")) if dedup else len(events)
    checks = [
        Check("scans in windows + dropped = scans read", result["totals flink"]["scans"] + dropped, read, 0),
    ]
    if lateness:
        checks.append(Check("scans dropped", dropped, 0, 0))
    if dedup:
        checks.append(Check("windows exact %", round(100 * result["exact"] / result["truth windows"], 2), 100, 0))
    facts = {
        "job": f"{job_id}, {job['duration'] / 1000:.1f} s; events sent in {sent_s:.1f} s at {args.rate:g}/s",
        "windows (truth, output)": f"{result['truth windows']:,}, {result['output windows']:,}; "
                                   f"{len(output):,} firings",
        "windows exact": f"{result['exact']:,} = {100 * result['exact'] / result['truth windows']:.2f}%",
        "windows over-counted, under-counted": f"{result['over-counted']:,}, {result['under-counted']:,}",
        "windows missing, extra": f"{result['missing']:,}, {result['extra']:,}",
        "totals truth": result["totals truth"],
        "totals flink": result["totals flink"],
        "scans dropped (Flink, side output)": f"{dropped:,}",
        "scans dropped with a watermark after every record (simulated)": f"{simulated_drops(events, lateness, dedup):,}",
    }
    print(f"mode {args.mode}: allowed lateness {lateness // 60_000} min, dedup {'on' if dedup else 'off'}")
    for c in checks:
        print(f"{'PASS' if c.passed else 'FAIL'}  {c.name}  {c.measured:,.0f}  target {c.target:,.0f}")
    for key, value in facts.items():
        print(f"{key}: {value}")
    sys.exit(0 if all(c.passed for c in checks) else 1)


if __name__ == "__main__":
    main()
