"""Layout of the overview diagram (overview.json), read by draw.py.

The batch steps sit on the top row and the zones they write on the row below, so every write is
an arrow down and every read an arrow up. The stream and the services around the pipelines
(Airflow, DataHub, the storage servers) are bands below. Coordinates are in SVG pixels.
"""

W, H = 2240, 1110
SW, SH = 280, 112          # a step
ZW, ZH = 280, 84           # a zone
STEP_Y, ZONE_Y, ROW3_Y = 140, 345, 492
STEPS = {"sources": 40, "dp1": 380, "silver-step": 720, "gold-step": 1060, "dp3": 1400, "trino": 1740}
ZONES = {"bronze": 550, "silver": 890, "gold": 1230, "features": 1570, "metastore": 1910}
STREAM_Y, BAND_Y, BAND_H = 690, 830, 72


def step(sid, tag, lines, style="unit", icon="compute"):
    """Return a step box on the top row."""
    w = 230 if sid == "sources" else SW
    return {"x": STEPS[sid], "y": STEP_Y, "w": w, "h": SH, "tag": tag, "icon": icon, "lines": lines, "style": style}


def zone(zid, tag, lines, icon="storage"):
    """Return a zone box on the store row."""
    return {"x": ZONES[zid], "y": ZONE_Y, "w": ZW, "h": ZH, "tag": tag, "icon": icon, "lines": lines, "style": "store"}


def band(y, tag, lines, icon, style="unit"):
    """Return a full-width band."""
    return {"x": 20, "y": y, "w": 2200, "h": BAND_H, "tag": tag, "icon": icon, "lines": lines, "style": style}


def down(x, y0, y1, text):
    """Return a vertical arrow at x from y0 to y1, its text right of the line."""
    return {"route": [[x, y0], [x, y1]], "badge": [x + 12, (y0 + y1) / 2], "text": "right"}


boxes = {
    "sources": step("sources", "source", ["Postgres oms: orders, couriers,", "hubs, customers, courier changes",
                                          "s3://app-exports: pings, scans"], icon="database"),
    "dp1": step("dp1", "01 · DP1", ["JDBC: the rows changed since", "the watermark, the new export dates",
                                    "Great Expectations: 73 checks"]),
    "silver-step": step("silver-step", "02 · DP2", ["oms changes merged, the 2% pings", "deduplicated, v1 and v2 unified",
                                                    "OPTIMIZE ZORDER BY (courier_id)"]),
    "gold-step": step("gold-step", "03 · DP2", ["SCD2 dim_courier, fact_delivery", "NULL-isolation join: late_trip_min",
                                                "Great Expectations: 62 checks"]),
    "dp3": step("dp3", "05 · DP3 · NOVEL 2", ["reads only the facts' Change", "Data Feed: 3 feat_ tables merged",
                                               "31 checks, = a full recompute"], style="novel"),
    "trino": step("trino", "06 · SQL", ["SQL on every zone, :8090", "the tables from the catalog"]),
    "bronze": zone("bronze", "s3 lakehouse/bronze", ["raw_* Delta tables", "the sources as they are"]),
    "silver": zone("silver", "s3 lakehouse/silver", ["stg_* Delta tables", "clean, one row per id"]),
    "gold": zone("gold", "s3 lakehouse/gold", ["dim_* and fact_* Delta tables", "SCD2 courier, H3 zones"]),
    "features": zone("features", "s3 lakehouse/gold/feat_*", ["feat_* Delta tables", "event_timestamp, created"]),
    "metastore": zone("metastore", "catalog", ["the tables of every zone", "for Spark and Trino"], icon="database"),
    "h3": {"x": 890, "y": ROW3_Y, "w": 280, "h": 100, "tag": "NOVEL 1", "icon": "search", "style": "novel",
           "lines": ["ST_H3CellIDs, resolution 7 (5 km²)", "1,497 drop zones in dim_zone"]},
    "warehouse-step": {"x": 1230, "y": ROW3_Y, "w": 280, "h": 100, "tag": "04 · DP2", "icon": "compute",
                       "lines": ["gold to dwh staging, then", "one swap in one transaction"]},
    "dwh": {"x": 1570, "y": ROW3_Y + 8, "w": 280, "h": ZH, "tag": "postgres dwh", "icon": "database", "style": "store",
            "lines": ["star schema + obt_delivery", "B-tree, partial, BRIN indexes"]},
    "producer": {"x": 40, "y": STREAM_Y, "w": 250, "h": 86, "tag": "host", "icon": "process",
                 "lines": ["a week of scans: 8% late", "and 1.5% retries"]},
    "scans-topic": {"x": 400, "y": STREAM_Y, "w": 280, "h": 86, "tag": "source topic", "icon": "queue", "style": "store",
                    "lines": ["record time = event time", "kept for ever (retention -1)"]},
    "flink": {"x": 790, "y": STREAM_Y, "w": 320, "h": 86, "tag": "07 · STREAM", "icon": "stream",
              "lines": ["5-minute windows per hub, watermark 10 s", "lateness 20 min, keyed dedup (TTL 15 min)"]},
    "out-topics": {"x": 1220, "y": STREAM_Y, "w": 300, "h": 86, "tag": "topics", "icon": "queue", "style": "store",
                   "lines": ["hub-delivery-5m: counts per window", "scans-late: the side output"]},
    "airflow": band(BAND_Y, "orchestration", ["DP1 → DP2 → DP3, daily: every DAG has an ingest stage and a validate "
                                              "stage (Great Expectations) · connections, variables, pool spark (1 slot)"],
                    "scheduler"),
    "datahub": band(BAND_Y + BAND_H + 12, "governance", ["the schemas of every zone · the lineage of every Airflow task "
                                                         "· the validation results as assertions"], "catalog"),
    "storage": band(BAND_Y + 2 * (BAND_H + 12), "storage", ["SeaweedFS (S3): app-exports, lakehouse · Postgres: oms, dwh, "
                                                             "airflow, metastore, datahub · Kafka: parcel-scans, "
                                                             "hub-delivery-5m, scans-late"], "storage", "store"),
}

sy = STEP_Y + SH                     # bottom of the steps
mid = STEP_Y + SH / 2
edges = {
    "o1": {"route": [[270, mid], [380, mid]], "badge": [325, mid], "text": "above"},
    "o2": down(600, sy, ZONE_Y, "writes"),
    "o3": {**down(780, ZONE_Y, sy, "reads")},
    "o4": down(940, sy, ZONE_Y, "writes"),
    "o5": down(1120, ZONE_Y, sy, "reads"),
    "o6": down(1280, sy, ZONE_Y, "writes"),
    "o7": {"route": [[1170, ROW3_Y + 30], [1200, ROW3_Y + 30], [1200, 372], [1230, 372]],
           "badge": [1186, 462], "text": "left"},
    "o8": down(1460, ZONE_Y, sy, "change feed"),
    "o9": down(1620, sy, ZONE_Y, "merges"),
    "o10": down(1300, ZONE_Y + ZH, ROW3_Y, "reads"),
    "o11": {"route": [[1510, ROW3_Y + 50], [1570, ROW3_Y + 50]], "badge": [1540, ROW3_Y + 50], "text": "above"},
    "o12": down(1960, ZONE_Y, sy, "tables"),
    "o13": {"route": [[290, STREAM_Y + 43], [400, STREAM_Y + 43]], "badge": [345, STREAM_Y + 43], "text": "above"},
    "o14": {"route": [[680, STREAM_Y + 43], [790, STREAM_Y + 43]], "badge": [735, STREAM_Y + 43], "text": "above"},
    "o15": {"route": [[1110, STREAM_Y + 43], [1220, STREAM_Y + 43]], "badge": [1165, STREAM_Y + 43], "text": "above"},
}

LAYOUT = {
    "canvas": [W, H],
    "eyebrow": "LASTLEG LOGISTICS · OVERVIEW",
    "keys": [("store", "storage: where the data rests"), ("arrow", "data moves this way"),
             ("novel", "novel idea (not taught in EDAI)")],
    "panels": [
        {"label": "BATCH · AIRFLOW, DAILY", "note": "each step writes a zone; the next step reads it from there",
         "x": 20, "y": 100, "w": 2200, "h": 520},
        {"label": "LAKEHOUSE", "note": "Delta tables in s3://lakehouse, registered in the Hive Metastore",
         "x": 530, "y": 300, "w": 1680, "h": 162, "dashed": True, "label_at": "bottom"},
        {"label": "STREAM · REPLAY OF THE PARCEL SCANS", "note": "5-minute counts per hub, checked against the truth",
         "x": 20, "y": 646, "w": 2200, "h": 156},
    ],
    "boxes": boxes,
    "edges": edges,
    "flows": {},
}
