"""Create Airflow's connections, variables and pool from the environment (run by airflow-init).

The source credentials reach only the init container (env_file: .env). They are stored as Airflow
connections, encrypted with the Fernet key, so DAG code never holds a password. Running it again
updates the same entries.
"""

import json
import os

from airflow.models import Connection, Pool, Variable
from airflow.utils.session import create_session


def connections() -> list[Connection]:
    """Return the platform's connections, filled from the environment."""
    pg = {"conn_type": "postgres", "host": "postgres", "port": 5432,
          "login": os.environ["POSTGRES_USER"], "password": os.environ["POSTGRES_PASSWORD"]}
    return [
        Connection(conn_id="oms_postgres", schema="oms", description="Order system (source)", **pg),
        Connection(conn_id="dwh_postgres", schema="dwh", description="Warehouse", **pg),
        Connection(
            conn_id="lakehouse_s3", conn_type="aws", description="SeaweedFS S3 (app-exports, lakehouse)",
            login=os.environ["S3_ACCESS_KEY"], password=os.environ["S3_SECRET_KEY"],
            extra=json.dumps({"endpoint_url": "http://seaweedfs:8333", "region_name": "us-east-1"}),
        ),
        Connection(
            conn_id="spark_lakehouse", conn_type="spark", host="spark://spark-master", port=7077,
            description="Spark standalone master", extra=json.dumps({"deploy-mode": "client"}),
        ),
    ]


VARIABLES = {
    "jobs_dir": "/opt/jobs",
    "lakehouse_bucket": "lakehouse",
    "exports_bucket": "app-exports",
    "period_start": "2024-06-01",
    "period_end": "2024-11-30",
}


def main() -> None:
    """Create or update every connection, variable and the spark pool."""
    with create_session() as session:
        for conn in connections():
            session.query(Connection).filter(Connection.conn_id == conn.conn_id).delete()
            session.add(conn)
            print(f"connection {conn.conn_id} ({conn.conn_type})")
        pool = session.query(Pool).filter(Pool.pool == "spark").one_or_none() or Pool(pool="spark")
        pool.slots = 1
        pool.description = "one Spark application at a time (one 4-core worker)"
        pool.include_deferred = False
        session.merge(pool)
        print("pool spark (1 slot)")
    for key, value in VARIABLES.items():
        Variable.set(key, value)
        print(f"variable {key}")


if __name__ == "__main__":
    main()
