"""Postgres access for the generator: connect with the .env credentials and bulk-load DataFrames.

The connection settings are never printed. Host and port default to the core profile's
127.0.0.1:15432 and can be changed with POSTGRES_HOST and POSTGRES_PORT.
"""

from __future__ import annotations

import io
import os
from pathlib import Path

import pandas as pd
import psycopg

ENV_FILE = Path(__file__).resolve().parent.parent / ".env"
COPY_CHUNK_ROWS = 500_000


def read_env(path: Path = ENV_FILE, prefixes: tuple[str, ...] = ("POSTGRES_",)) -> dict[str, str]:
    """Return KEY=VALUE pairs from the .env file, overridden by process variables with these prefixes."""
    values: dict[str, str] = {}
    if path.exists():
        for line in path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip().strip('"').strip("'")
    values.update({k: v for k, v in os.environ.items() if k.startswith(prefixes)})
    return values


def connect(dbname: str) -> psycopg.Connection:
    """Open a connection to one of the platform's databases as the .env user."""
    env = read_env()
    missing = [k for k in ("POSTGRES_USER", "POSTGRES_PASSWORD") if not env.get(k)]
    if missing:
        raise RuntimeError(f"set {', '.join(missing)} in .env (copy .env.example)")
    return psycopg.connect(
        host=env.get("POSTGRES_HOST", "127.0.0.1"),
        port=int(env.get("POSTGRES_PORT", "15432")),
        user=env["POSTGRES_USER"],
        password=env["POSTGRES_PASSWORD"],
        dbname=dbname,
    )


def copy_frame(cur: psycopg.Cursor, table: str, frame: pd.DataFrame) -> None:
    """Bulk-load a DataFrame into table with COPY (CSV), in chunks, columns matched by name."""
    columns = ", ".join(frame.columns)
    with cur.copy(f"COPY {table} ({columns}) FROM STDIN (FORMAT csv)") as copy:
        for start in range(0, len(frame), COPY_CHUNK_ROWS):
            buffer = io.StringIO()
            frame.iloc[start:start + COPY_CHUNK_ROWS].to_csv(buffer, index=False, header=False)
            copy.write(buffer.getvalue())
