"""Shared task factory for the DP1 to DP3 DAGs: one Spark job submitted to the lakehouse cluster.

Credentials come from Airflow connections at run time (templated env_vars), never from this code.
The driver runs in the scheduler container (client mode); the executors run on spark-worker.
"""

from datetime import datetime, timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Runs go to 2024-12-01: parcels ordered on 2024-11-30 and delivered after midnight are changed, and
# their scans exported, on 2024-12-01.
PERIOD_START, PERIOD_END = datetime(2024, 6, 1), datetime(2024, 12, 1)
DEFAULT_ARGS = {"owner": "lastleg-data", "retries": 1, "retry_delay": timedelta(minutes=2)}

SPARK_ENV = {
    "POSTGRES_USER": "{{ conn.oms_postgres.login }}",
    "POSTGRES_PASSWORD": "{{ conn.oms_postgres.password }}",
    "AWS_ACCESS_KEY_ID": "{{ conn.lakehouse_s3.login }}",
    "AWS_SECRET_ACCESS_KEY": "{{ conn.lakehouse_s3.password }}",
    "PYSPARK_DRIVER_PYTHON": "/opt/driver-python/bin/python",
}
SPARK_CONF = {
    "spark.driver.host": "airflow-scheduler",  # the executors connect back to the driver here
    "spark.driver.bindAddress": "0.0.0.0",
}


def spark_job(task_id: str, job: str, args: list[str], retries: int | None = None) -> SparkSubmitOperator:
    """Return a task that runs jobs/<job> with spark-submit in the one-slot spark pool."""
    extra = {} if retries is None else {"retries": retries}
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id="spark_lakehouse",
        application="{{ var.value.jobs_dir }}/" + job,
        application_args=args,
        name=f"{task_id}_{{{{ ds_nodash }}}}",
        env_vars=SPARK_ENV,
        conf=SPARK_CONF,
        pool="spark",
        **extra,
    )
