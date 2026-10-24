"""Measure the dwh.obt_delivery indexes: each query's plan without and with its index.

    uv run python scripts/dwh_indexes.py

For each query the script runs EXPLAIN (ANALYZE, BUFFERS) three times and keeps the last, so both
sides are measured with a warm cache. "Without" drops the three indexes inside a transaction that is
rolled back, so the warehouse is never changed. It prints the scan used, the pages read (shared hit
plus read) and the execution time, then the size of each index.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from generator import db  # noqa: E402

INDEXES = ["obt_delivery_hub_promised", "obt_delivery_late_hub_delivered", "obt_delivery_order_ts_brin"]
QUERIES = {
    "one hub's deliveries promised on one day (B-tree on hub_id, promised_ts)": """
        SELECT count(*), round(avg(delivery_min)::numeric, 1), round(100.0 * avg(is_late::int), 1)
        FROM obt_delivery
        WHERE hub_id = 7 AND promised_ts >= '2024-11-20 08:00+00' AND promised_ts < '2024-11-21 08:00+00'""",
    "a hub's 20 latest late deliveries (partial index on hub_id, delivered_ts WHERE is_late)": """
        SELECT order_id, courier_id, delivered_ts, delivery_min - promised_min AS minutes_late
        FROM obt_delivery
        WHERE is_late AND hub_id = 1 AND delivered_ts >= '2024-11-29 08:00+00'
        ORDER BY delivered_ts DESC LIMIT 20""",
    "orders per hub in a two-hour window (BRIN on order_ts)": """
        SELECT hub_id, count(*) FROM obt_delivery
        WHERE order_ts >= '2024-11-28 18:00+00' AND order_ts < '2024-11-28 20:00+00'
        GROUP BY hub_id ORDER BY 2 DESC LIMIT 5""",
}


def scans(node: dict) -> list[str]:
    """Return the scan nodes of a plan, with their index, depth first."""
    found = [f"{node['Node Type']} ({node['Index Name']})" if "Index Name" in node else node["Node Type"]] \
        if "Scan" in node["Node Type"] else []
    for child in node.get("Plans", []):
        found += scans(child)
    return found


def explain(cur, sql: str) -> tuple[str, int, float]:
    """Return the scans, the pages read and the execution time (ms) of the warm third run."""
    for _ in range(3):
        cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}")
        result = cur.fetchone()[0][0]
    plan = result["Plan"]
    return ", ".join(scans(plan)), plan["Shared Hit Blocks"] + plan["Shared Read Blocks"], result["Execution Time"]


def main() -> None:
    """Print each query without and with the indexes, then the index sizes."""
    with db.connect("dwh") as conn, conn.cursor() as cur:
        cur.execute("ANALYZE obt_delivery")
        conn.commit()
        cur.execute("SELECT count(*), pg_relation_size('obt_delivery') / 8192 FROM obt_delivery")
        rows, pages = cur.fetchone()
        cur.execute("SELECT correlation FROM pg_stats WHERE tablename = 'obt_delivery' AND attname = 'order_ts'")
        print(f"obt_delivery: {rows:,} rows in {pages:,} pages; order_ts correlation {cur.fetchone()[0]:.3f}\n")
        for name, sql in QUERIES.items():
            cur.execute("BEGIN")
            cur.execute("".join(f"DROP INDEX {i};" for i in INDEXES))
            without = explain(cur, sql)
            cur.execute("ROLLBACK")
            with_index = explain(cur, sql)
            conn.commit()
            print(name)
            for label, (plan, read, ms) in (("without", without), ("with", with_index)):
                print(f"  {label:<8} {read:>6,} pages  {ms:>8.2f} ms  {plan}")
        cur.execute("SELECT indexrelname, pg_size_pretty(pg_relation_size(indexrelid)) FROM pg_stat_user_indexes "
                    "WHERE relname = 'obt_delivery' ORDER BY 1")
        print("\nindex sizes:", dict(cur.fetchall()))


if __name__ == "__main__":
    main()
