"""One-day dry run at full scale, in memory: time and bytes per row, then the full-run projection.

    uv run python -m generator.dry_run --workers 6

Builds the full-scale reference data and one v1 day (2024-06-03) and one v2 day (2024-10-01) of
orders, scans and pings, serialises the files exactly as the export does, and uploads nothing.
The printed table fixes the full-scale parameters in docs/data_characteristics.md.
"""

from __future__ import annotations

import argparse
import time
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

from generator import db
from generator.app_exports import (
    COURIERS_PER_FILE,
    DUPLICATE_SHARE,
    PINGS_PER_SHIFT,
    V2_ROLLOUT_DAYS,
    V2_ROLLOUT_START,
    _scan_table,
    build_ping_files,
    build_scans,
    parquet_bytes,
)
from generator.effect_model import fit, load_seed
from generator.la_config import TIME_ZONE
from generator.oms_orders import build_day, courier_state_at, local_days, orders_per_day, seed_patterns
from generator.oms_reference import DEFAULT_SEED, build_reference

DRY_DAYS = ("2024-06-03", "2024-10-01")


def _ping_bytes(day: pd.Timestamp, day_index: int, part: int, couriers: pd.DataFrame, trips: dict) -> tuple[int, int, float]:
    """Build one courier group's ping files without uploading; return (rows, bytes, seconds)."""
    started = time.perf_counter()
    files = build_ping_files(day, day_index, couriers, trips, np.random.default_rng([DEFAULT_SEED, 4, day_index, part]))
    blobs = [parquet_bytes(table) for _, table in files]
    return sum(t.num_rows for _, t in files), sum(len(b) for b in blobs), time.perf_counter() - started


def main() -> None:
    """Run the dry run and print the measured rates and the full-scale projection."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--workers", type=int, default=6)
    args = parser.parse_args()
    t0 = time.perf_counter()
    reference = build_reference(1.0)
    t_reference = time.perf_counter() - t0
    model, patterns = fit(load_seed()), seed_patterns()
    days, counts = local_days(), orders_per_day(1.0)
    measured = {}
    for label in DRY_DAYS:
        day = pd.Timestamp(label, tz=TIME_ZONE)
        i = list(days).index(day)
        t1 = time.perf_counter()
        orders = build_day(day, int(counts.iloc[i]), 1, reference, model, patterns, np.random.default_rng([DEFAULT_SEED, 3, i]))
        t_orders = time.perf_counter() - t1
        scan_blob = parquet_bytes(_scan_table(build_scans(orders, np.random.default_rng([DEFAULT_SEED, 5, i]))))
        state = courier_state_at(reference["courier_changes"], reference["couriers"], day.tz_convert("UTC")).sort_values("courier_id")
        trips = {int(c): g.order_id.to_numpy() for c, g in orders.sort_values("pickup_ts").groupby("courier_id")}
        t2 = time.perf_counter()
        with ProcessPoolExecutor(args.workers) as pool:
            futures = []
            for part, start in enumerate(range(0, len(state), COURIERS_PER_FILE)):
                chunk = state.iloc[start:start + COURIERS_PER_FILE][["courier_id", "home_hub_id", "vehicle_type"]]
                futures.append(pool.submit(_ping_bytes, day, i, part, chunk, {c: trips[c] for c in chunk.courier_id if c in trips}))
            results = [f.result() for f in futures]
        t_pings = time.perf_counter() - t2
        rows, size = sum(r[0] for r in results), sum(r[1] for r in results)
        measured[label] = {
            "orders": len(orders), "orders_s": t_orders, "scan_b_per_row": len(scan_blob) / (3 * len(orders)),
            "active_couriers": len(state), "files": len(results), "ping_rows": rows, "ping_b_per_row": size / rows,
            "pings_s": t_pings, "ping_rows_per_s": rows / t_pings,
        }
        print(f"{label}: {measured[label]}")

    with db.connect("oms") as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_total_relation_size('orders')::float / count(*) FROM orders")
        orders_b = cur.fetchone()[0]
    hired = reference["couriers"].hired_ts
    active = np.array([(hired < d.tz_convert("UTC")).sum() for d in days])
    v2_day = np.array([(d - V2_ROLLOUT_START).days >= V2_ROLLOUT_DAYS - 1 for d in days])
    v1_b, v2_b = measured[DRY_DAYS[0]]["ping_b_per_row"], measured[DRY_DAYS[1]]["ping_b_per_row"]
    ping_rows = active * PINGS_PER_SHIFT * (1 + DUPLICATE_SHARE)
    ping_bytes = float((ping_rows * np.where(v2_day, v2_b, v1_b)).sum())
    scan_bytes = 3 * counts.sum() * np.mean([m["scan_b_per_row"] for m in measured.values()])
    oms_bytes = orders_b * counts.sum()
    per_day_s = np.mean([m["orders_s"] + m["pings_s"] for m in measured.values()])
    print(f"reference data at full scale built in {t_reference:.1f} s")
    print(f"projection: pings {ping_rows.sum() / 1e9:.2f}B rows, {ping_bytes / 1e9:.1f} GB; scans {3 * counts.sum() / 1e6:.0f}M rows, "
          f"{scan_bytes / 1e9:.1f} GB; oms orders {counts.sum() / 1e6:.1f}M rows at {orders_b:.0f} B/row, {oms_bytes / 1e9:.1f} GB; "
          f"total {(ping_bytes + scan_bytes + oms_bytes) / 1e9:.1f} GB")
    print(f"projection: export about {per_day_s * len(days) / 60:.0f} min of generation for {len(days)} days with {args.workers} workers (upload not included)")


if __name__ == "__main__":
    main()
