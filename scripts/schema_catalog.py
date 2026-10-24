"""Read the live catalog of every zone: check the table names and draw one relationship diagram per zone.

    uv run python scripts/schema_catalog.py --check-naming
    uv run python scripts/schema_catalog.py --er-diagrams

The lakehouse tables come from Trino (catalog delta, schemas bronze, silver and gold) and the
warehouse tables from Postgres (dwh, schema public). The naming check needs raw_ in bronze, stg_ in
silver, dim_/fact_/obt_/feat_ in gold and dim_/fact_/obt_ in dwh, and exits 1 on any other name.

The diagrams (Graphviz, docs/images/schema/<zone>-er.png) show each table with its key columns.
The warehouse's relationships are its declared foreign keys. Delta tables declare none, so the
lakehouse relationships are the logical model below, and each one is checked against the live
columns before it is drawn. A dashed edge joins on a natural key that matches several rows of an
SCD2 dimension or a change history (courier_id against dim_courier or stg_courier_changes).
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import urllib.request
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
from generator import db  # noqa: E402

TRINO = "http://127.0.0.1:8090/v1/statement"
OUT = REPO / "docs" / "images" / "schema"
PREFIXES = {
    "bronze": ("raw_",),
    "silver": ("stg_",),
    "gold": ("dim_", "fact_", "obt_", "feat_"),
    "dwh": ("dim_", "fact_", "obt_"),
}
EXCEPTIONS = {"gold.dp3_progress": "bookkeeping of the incremental DP3 run (the fact versions it processed), not data"}
KEY_COLUMNS = ("event_timestamp", "created", "valid_from_ts", "valid_to_ts", "is_current")

SOURCE_MODEL = [
    ("orders.customer_id", "customers.customer_id"),
    ("orders.hub_id", "hubs.hub_id"),
    ("orders.courier_id", "couriers.courier_id"),
    ("couriers.home_hub_id", "hubs.hub_id"),
    ("customers.service_hub_id", "hubs.hub_id"),
    ("courier_changes.courier_id", "couriers.courier_id"),
    ("courier_pings.courier_id", "couriers.courier_id"),
    ("courier_pings.trip_id", "orders.order_id"),
    ("parcel_scans.order_id", "orders.order_id"),
]
LOGICAL_MODEL = {
    "bronze": [(f"raw_{a}", f"raw_{b}") for a, b in SOURCE_MODEL],
    # Silver has no courier table: couriers arrive as their change history, which DP2 turns into the
    # SCD2 dim_courier, so a courier reference joins stg_courier_changes on courier_id.
    "silver": [
        ("stg_orders.customer_id", "stg_customers.customer_id"),
        ("stg_orders.hub_id", "stg_hubs.hub_id"),
        ("stg_orders.courier_id", "stg_courier_changes.courier_id"),
        ("stg_customers.service_hub_id", "stg_hubs.hub_id"),
        ("stg_courier_changes.home_hub_id", "stg_hubs.hub_id"),
        ("stg_courier_pings.courier_id", "stg_courier_changes.courier_id"),
        ("stg_courier_pings.trip_id", "stg_orders.order_id"),
        ("stg_parcel_scans.order_id", "stg_orders.order_id"),
    ],
    "gold": [
        ("fact_delivery.courier_sk", "dim_courier.courier_sk"),
        ("fact_delivery.hub_id", "dim_hub.hub_id"),
        ("fact_delivery.customer_id", "dim_customer.customer_id"),
        ("fact_delivery.date_key", "dim_date.date_key"),
        ("fact_delivery.drop_zone_id", "dim_zone.zone_id"),
        ("fact_courier_activity_hourly.hub_id", "dim_hub.hub_id"),
        ("fact_courier_activity_hourly.date_key", "dim_date.date_key"),
        ("fact_courier_activity_hourly.courier_id", "dim_courier.courier_id"),
        ("dim_courier.hub_id", "dim_hub.hub_id"),
        ("dim_customer.hub_id", "dim_hub.hub_id"),
        ("dim_zone.main_hub_id", "dim_hub.hub_id"),
        ("feat_courier_daily.courier_id", "dim_courier.courier_id"),
        ("feat_hub_hourly.hub_id", "dim_hub.hub_id"),
        ("feat_zone_daily.zone_id", "dim_zone.zone_id"),
    ],
}
NATURAL_KEYS = {"dim_courier.courier_id", "stg_courier_changes.courier_id"}


def trino(sql: str) -> list[list]:
    """Return every row of a Trino query."""
    request = urllib.request.Request(TRINO, data=sql.encode(), headers={"X-Trino-User": "schema-catalog"})
    response, rows = json.load(urllib.request.urlopen(request, timeout=30)), []
    while True:
        rows += response.get("data", [])
        if "error" in response:
            raise RuntimeError(response["error"]["message"])
        if "nextUri" not in response:
            return rows
        response = json.load(urllib.request.urlopen(response["nextUri"], timeout=30))


def catalog() -> tuple[dict[str, dict[str, list[str]]], dict[str, set[str]], list[tuple[str, str]]]:
    """Return {zone: {table: columns}}, the dwh primary-key columns, and the dwh foreign keys."""
    tables: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    for zone, table, column in trino(
        "SELECT table_schema, table_name, column_name FROM delta.information_schema.columns "
        "WHERE table_schema IN ('bronze', 'silver', 'gold') ORDER BY 1, 2, ordinal_position"
    ):
        tables[zone][table].append(column)
    with db.connect("dwh") as conn, conn.cursor() as cur:
        cur.execute("SELECT table_name, column_name FROM information_schema.columns "
                    "WHERE table_schema = 'public' ORDER BY table_name, ordinal_position")
        for table, column in cur.fetchall():
            tables["dwh"][table].append(column)
        cur.execute("""SELECT tc.constraint_type, kcu.table_name, kcu.column_name, ccu.table_name, ccu.column_name
                       FROM information_schema.table_constraints tc
                       JOIN information_schema.key_column_usage kcu USING (constraint_name, table_schema)
                       JOIN information_schema.constraint_column_usage ccu USING (constraint_name, table_schema)
                       WHERE tc.table_schema = 'public' AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY')""")
        keys = cur.fetchall()
    primary = {f"{t}.{c}" for kind, t, c, _, _ in keys if kind == "PRIMARY KEY"}
    foreign = [(f"{t}.{c}", f"{rt}.{rc}") for kind, t, c, rt, rc in keys if kind == "FOREIGN KEY"]
    return tables, primary, foreign


def check_naming(tables: dict[str, dict[str, list[str]]]) -> int:
    """Print every table with its verdict and return the number of names that break the convention."""
    bad = 0
    for zone, prefixes in PREFIXES.items():
        print(f"{zone}: {' or '.join(prefixes)}")
        for table in sorted(tables[zone]):
            name = f"{zone}.{table}"
            if name in EXCEPTIONS:
                print(f"  EXCEPTION  {table}: {EXCEPTIONS[name]}")
            elif table.startswith(prefixes):
                print(f"  PASS       {table}")
            else:
                print(f"  FAIL       {table}")
                bad += 1
    total = sum(len(t) for t in tables.values())
    print(f"\n{total - bad - len(EXCEPTIONS)} of {total} tables follow the convention, "
          f"{len(EXCEPTIONS)} documented exception, {bad} violations")
    return bad


def dot_for(zone: str, columns: dict[str, list[str]], edges: list[tuple[str, str]], primary: set[str]) -> str:
    """Return the Graphviz source of one zone: tables with their key columns, and the relationships."""
    linked = {end for edge in edges for end in edge}
    lines = [f'digraph "{zone}" {{', "  rankdir=LR; nodesep=0.4; ranksep=1.1;",
             '  node [shape=plaintext fontname="Helvetica" fontsize=11]; edge [color="#5b6b7f" arrowsize=0.7];']
    for table, cols in sorted(columns.items()):
        if f"{zone}.{table}" in EXCEPTIONS:
            continue
        keys = [c for c in cols if f"{table}.{c}" in linked | primary or c.endswith(("_id", "_sk", "_key")) or c in KEY_COLUMNS]
        rows = "".join(
            f'<tr><td align="left" port="{c}">{"<b>" + c + "</b>" if f"{table}.{c}" in primary else c}</td></tr>' for c in keys
        )
        more = f'<tr><td align="left"><font color="#7a7a7a">+ {len(cols) - len(keys)} more columns</font></td></tr>' if len(cols) > len(keys) else ""
        lines.append(f'  "{table}" [label=<<table border="0" cellborder="1" cellspacing="0" cellpadding="4">'
                     f'<tr><td bgcolor="#dbe7f5"><b>{table}</b></td></tr>{rows}{more}</table>>];')
    for child, parent in edges:
        (ct, cc), (pt, pc) = child.split("."), parent.split(".")
        style = ' [style=dashed label="natural key" fontsize=9 fontcolor="#5b6b7f"]' if parent in NATURAL_KEYS else ""
        lines.append(f'  "{ct}":"{cc}" -> "{pt}":"{pc}"{style};')
    lines.append("}")
    return "\n".join(lines)


def er_diagrams(tables, primary: set[str], foreign: list[tuple[str, str]]) -> None:
    """Write one PNG per zone, after checking every logical relationship against the live columns."""
    OUT.mkdir(parents=True, exist_ok=True)
    for zone in ("bronze", "silver", "gold", "dwh"):
        edges = foreign if zone == "dwh" else LOGICAL_MODEL[zone]
        missing = [end for edge in edges for end in edge
                   if end.split(".")[1] not in tables[zone].get(end.split(".")[0], [])]
        if missing:
            raise SystemExit(f"{zone}: the logical model names columns that do not exist: {missing}")
        source = dot_for(zone, tables[zone], edges, primary if zone == "dwh" else set())
        path = OUT / f"{zone}-er.png"
        subprocess.run(["dot", "-Tpng", "-Gdpi=130", "-o", str(path)], input=source, text=True, check=True)
        print(f"{zone}: {len(tables[zone])} tables, {len(edges)} relationships -> {path.relative_to(REPO)}")


def main() -> None:
    """Run the naming check, the diagrams, or both."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check-naming", action="store_true")
    parser.add_argument("--er-diagrams", action="store_true")
    args = parser.parse_args()
    if not (args.check_naming or args.er_diagrams):
        parser.error("choose --check-naming, --er-diagrams or both")
    tables, primary, foreign = catalog()
    if args.er_diagrams:
        er_diagrams(tables, primary, foreign)
    if args.check_naming and check_naming(tables):
        sys.exit(1)


if __name__ == "__main__":
    main()
