"""Layout of the system diagram (system.json), read by draw.py.

LAYOUT holds the canvas, the panels, a box per unit, the corner points of every arrow and where its
numbered badge sits, in SVG pixels. The units sit on a grid of 6 columns and 3 rows between the
Postgres bar (top) and the SeaweedFS bar (bottom).
"""

W, H = 2420, 1140
C = [316, 622, 928, 1234, 1540, 1846]          # column x, box width 190
BW, BH = 190, 86
R = [304, 500, 696]                             # row y
RX = 2124                                       # right panel cards
LX = 40                                         # left panel card


def box(col, row, tag, icon, lines, style="unit"):
    """Return a unit box in grid cell (col, row)."""
    return {"x": C[col], "y": R[row], "w": BW, "h": BH, "tag": tag, "icon": icon, "lines": lines, "style": style}


def cx(col):
    """Return the x of a column's centre."""
    return C[col] + BW / 2


def cy(row):
    """Return the y of a row's centre."""
    return R[row] + BH / 2


boxes = {
    "generator": {"x": LX, "y": R[1], "w": 196, "h": BH, "tag": "host", "icon": "process",
                  "lines": ["generator/*.py", "--scale 1.0: 110.19 GB"]},
    "postgres": {"x": 300, "y": 152, "w": 1736, "h": 70, "tag": "core", "icon": "database", "style": "store",
                 "lines": ["oms (the order system, a source)  ·  dwh (the star schema)  ·  "
                           "airflow, metastore, datahub (metadata of the services)"]},
    "seaweedfs": {"x": 300, "y": 852, "w": 1736, "h": 70, "tag": "core", "icon": "storage", "style": "store",
                  "lines": ["s3://app-exports (the app's export files, a source)  ·  "
                            "s3://lakehouse: bronze, silver, gold, _validation (Delta Lake)"]},
    "history": box(1, 0, "batch", "ui", ["Spark UI :18080"]),
    "spark": box(0, 1, "batch", "compute", ["master + worker", "4 cores, 2 GB executor"]),
    "scheduler": box(1, 1, "orch", "scheduler", ["DP1 → DP2 → DP3", "Spark drivers (client)"]),
    "metastore": box(1, 2, "batch", "database", ["tables of every zone"]),
    "trino": box(2, 2, "query", "compute", ["SQL on the lakehouse", ":8090"]),
    "frontend": box(3, 0, "gov", "ui", ["catalog UI :9002"]),
    "gms": box(3, 1, "gov", "catalog", ["metadata service", ":8083"]),
    "opensearch": box(4, 1, "gov", "search", ["DataHub search index"]),
    "webserver": box(3, 2, "orch", "ui", ["UI :8082"], "support"),
    "flink": box(4, 2, "stream", "stream", ["jobmanager + taskmanager", "scan_windows.py"]),
    "kafka": box(5, 2, "core", "queue", ["parcel-scans", "hub-delivery-5m, scans-late"]),
    "analyst": {"x": RX, "y": 150, "w": 236, "h": BH, "tag": "user", "icon": "user", "lines": ["DBeaver, browser"]},
    "ingestion": {"x": RX, "y": 330, "w": 236, "h": BH, "tag": "host", "icon": "process",
                  "lines": ["datahub ingest", "3 recipes"]},
    "producer": {"x": RX, "y": R[2], "w": 236, "h": BH, "tag": "host", "icon": "process",
                 "lines": ["scan_producer.py", "stream_windows.py"]},
}

gx = LX + 98
edges = {
    "d1": {"route": [[gx, R[1]], [gx, 187], [300, 187]], "badge": [gx, 300], "text": "right"},
    "d2": {"route": [[gx, R[1] + BH], [gx, 887], [300, 887]], "badge": [gx, 720], "text": "right"},
    "b1": {"route": [[C[1], cy(1)], [C[0] + BW, cy(1)]], "badge": [(C[0] + BW + C[1]) / 2, cy(1)], "text": "above"},
    "b2": {"route": [[C[0] + 60, 222], [C[0] + 60, R[1]]], "badge": [C[0] + 60, 262], "text": "left"},
    "b6": {"route": [[C[0] + 140, R[1]], [C[0] + 140, 222]], "badge": [C[0] + 140, 420], "text": "right"},
    "b3": {"route": [[C[0] + 40, 852], [C[0] + 40, R[1] + BH]], "badge": [C[0] + 40, 660], "text": "left"},
    "b4": {"route": [[C[0] + 100, R[1] + BH], [C[0] + 100, 852]], "badge": [C[0] + 100, 790], "text": "right"},
    "b5": {"route": [[cx(1), R[1] + BH], [cx(1), R[2]]], "badge": [cx(1), 641], "text": "right"},
    "b7": {"route": [[cx(1), R[1]], [cx(1), R[0] + BH]], "badge": [cx(1), 445], "text": "right"},
    "b8": {"route": [[cx(2), 852], [cx(2), R[2] + BH]], "badge": [cx(2), 818], "text": "right"},
    "b9": {"route": [[C[1] + BW, cy(2)], [C[2], cy(2)]], "badge": [C[1] + BW + 40, cy(2)], "text": "above"},
    "b10": {"route": [[C[2] + 40, R[2]], [C[2] + 40, 252], [2200, 252], [2200, 236]],
            "badge": [C[2] + 40, 440], "text": "right"},
    "b11": {"route": [[2036, 200], [RX, 200]], "badge": [2080, 200], "text": "above"},
    "s1": {"route": [[RX, R[2] + 26], [C[5] + BW, R[2] + 26]], "badge": [(C[5] + BW + RX) / 2, R[2] + 26],
           "text": "above"},
    "s5": {"route": [[C[5] + BW, R[2] + 62], [RX, R[2] + 62]], "badge": [(C[5] + BW + RX) / 2, R[2] + 62],
           "text": "below"},
    "s2": {"route": [[RX + 118, R[2] + BH], [RX + 118, 818], [cx(4), 818], [cx(4), R[2] + BH]],
           "badge": [1960, 818], "text": "right"},
    "s3": {"route": [[C[5], R[2] + 26], [C[4] + BW, R[2] + 26]], "badge": [(C[4] + BW + C[5]) / 2, R[2] + 26],
           "text": "above"},
    "s4": {"route": [[C[4] + BW, R[2] + 62], [C[5], R[2] + 62]], "badge": [(C[4] + BW + C[5]) / 2, R[2] + 62],
           "text": "below"},
    "g1": {"route": [[2036, 214], [2088, 214], [2088, 350], [RX, 350]], "badge": [2088, 300], "text": "left"},
    "g2": {"route": [[C[2] + 150, R[2]], [C[2] + 150, 640], [2104, 640], [2104, 400], [RX, 400]],
           "badge": [1300, 640], "text": "above"},
    "g3": {"route": [[cx(5), R[2]], [cx(5), 660], [2396, 660], [2396, 373], [RX + 236, 373]],
           "badge": [2396, 560], "text": "left"},
    "g4": {"route": [[RX, 374], [1470, 374], [1470, R[1] + 20], [C[3] + BW, R[1] + 20]],
           "badge": [1700, 374], "text": "above"},
    "g5": {"route": [[C[1] + BW + 92, 852], [C[1] + BW + 92, R[1] + 66], [C[1] + BW, R[1] + 66]],
           "badge": [C[1] + BW + 92, 817], "text": "left"},
    "g6": {"route": [[C[1] + BW, R[1] + 24], [C[3], R[1] + 24]], "badge": [1130, R[1] + 24], "text": "above"},
    "g7": {"route": [[C[3] + BW, R[1] + 62], [C[4], R[1] + 62]], "badge": [(C[3] + BW + C[4]) / 2, R[1] + 62],
           "text": "below"},
    "g8": {"route": [[cx(3), R[1]], [cx(3), R[0] + BH]], "badge": [cx(3), 445], "text": "right"},
    "g9": {"route": [[cx(3), R[0]], [cx(3), 272], [2300, 272], [2300, 236]], "badge": [1700, 272], "text": "below"},
}

LAYOUT = {
    "canvas": [W, H],
    "eyebrow": "LASTLEG LOGISTICS · EDAI-1 DATA PLATFORM",
    "panels": [
        {"label": "MAC", "note": "host process", "x": 20, "y": 112, "w": 236, "h": 834, "dashed": True},
        {"label": "DOCKER DESKTOP VM", "note": "compose project late-delivery-prediction · tag = compose profile",
         "x": 280, "y": 112, "w": 1800, "h": 834},
        {"label": "MAC", "note": "host processes, users", "x": 2104, "y": 112, "w": 296, "h": 834, "dashed": True},
    ],
    "boxes": boxes,
    "edges": edges,
    "flows": {
        "D": {"name": "Data generation", "who": "developer, once per scale: the two sources", "color": "#9c36b5"},
        "B": {"name": "Batch pipelines", "who": "Airflow, daily: DP1 triggers DP2, DP2 triggers DP3", "color": "#1971c2"},
        "S": {"name": "Streaming", "who": "replay of parcel scans: Kafka → Flink windows", "color": "#e8590c"},
        "G": {"name": "Governance", "who": "DataHub: schemas, lineage, validation results", "color": "#2f9e44"},
    },
    "legend": {"y": 990, "notes": [
        "An arrow carries the data named on it, from the unit that sends it to the unit that receives it.",
        "B1 and S2 carry no data: they point from the caller to the unit it calls.",
        "The dashed box is a support unit whose arrows are left out: the Airflow webserver, which reads",
        "the airflow database and shows the DAG runs.",
    ]},
}
