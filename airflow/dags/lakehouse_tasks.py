"""Shared task factory for the DP1 to DP3 DAGs: one Spark job submitted to the lakehouse cluster.

Credentials, hosts and bucket names come from Airflow connections and variables at run time
(templated env_vars), never from this code. The driver runs in the scheduler container (client
mode); the executors run on spark-worker.
"""

from datetime import datetime, timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datahub_airflow_plugin.entities import Dataset

# Runs go to 2024-12-01: parcels ordered on 2024-11-30 and delivered after midnight are changed, and
# their scans exported, on 2024-12-01.
PERIOD_START, PERIOD_END = datetime(2024, 6, 1), datetime(2024, 12, 1)
DEFAULT_ARGS = {"owner": "lastleg-data", "retries": 1, "retry_delay": timedelta(minutes=2)}

SPARK_ENV = {
    "OMS_JDBC_URL": "jdbc:postgresql://{{ conn.oms_postgres.host }}:{{ conn.oms_postgres.port }}/{{ conn.oms_postgres.schema }}",
    "POSTGRES_USER": "{{ conn.oms_postgres.login }}",
    "POSTGRES_PASSWORD": "{{ conn.oms_postgres.password }}",
    "LAKEHOUSE_BUCKET": "{{ var.value.lakehouse_bucket }}",
    "EXPORTS_BUCKET": "{{ var.value.exports_bucket }}",
    "AWS_ACCESS_KEY_ID": "{{ conn.lakehouse_s3.login }}",
    "AWS_SECRET_ACCESS_KEY": "{{ conn.lakehouse_s3.password }}",
    "PYSPARK_DRIVER_PYTHON": "/opt/driver-python/bin/python",
}
# The warehouse task writes to dwh with the dwh_postgres connection instead.
DWH_ENV = {
    "DWH_JDBC_URL": "jdbc:postgresql://{{ conn.dwh_postgres.host }}:{{ conn.dwh_postgres.port }}/{{ conn.dwh_postgres.schema }}",
    "POSTGRES_USER": "{{ conn.dwh_postgres.login }}",
    "POSTGRES_PASSWORD": "{{ conn.dwh_postgres.password }}",
}
SPARK_CONF = {
    "spark.driver.host": "airflow-scheduler",  # the executors connect back to the driver here
    "spark.driver.bindAddress": "0.0.0.0",
}


OMS_TABLES = ["hubs", "couriers", "courier_changes", "customers", "orders"]
EXPORTS = ["courier_pings", "parcel_scans"]


def lake(zone: str, *tables: str) -> list[Dataset]:
    """Return lakehouse tables as DataHub datasets, named as the Trino ingestion names them."""
    return [Dataset("trino", f"delta.{zone}.{t}") for t in tables]


def postgres(database: str, *tables: str) -> list[Dataset]:
    """Return Postgres tables (schema public) as DataHub datasets."""
    return [Dataset("postgres", f"{database}.public.{t}") for t in tables]


def exports(*datasets: str) -> list[Dataset]:
    """Return the courier-app export folders in SeaweedFS as DataHub datasets."""
    return [Dataset("s3", f"app-exports/{d}") for d in datasets]


def spark_job(
    task_id: str, job: str, args: list[str], retries: int | None = None,
    inlets: list[Dataset] | None = None, outlets: list[Dataset] | None = None, env: dict | None = None,
) -> SparkSubmitOperator:
    """Return a task that runs jobs/<job> with spark-submit in the one-slot spark pool.

    inlets and outlets are the datasets the job reads and writes; the DataHub plugin sends them as
    the task's lineage. env adds to or replaces entries of SPARK_ENV for this task.
    """
    extra = {} if retries is None else {"retries": retries}
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id="spark_lakehouse",
        application="{{ var.value.jobs_dir }}/" + job,
        application_args=args,
        name=f"{task_id}_{{{{ ds_nodash }}}}",
        env_vars={**SPARK_ENV, **(env or {})},
        conf=SPARK_CONF,
        pool="spark",
        inlets=inlets or [],
        outlets=outlets or [],
        **extra,
    )
