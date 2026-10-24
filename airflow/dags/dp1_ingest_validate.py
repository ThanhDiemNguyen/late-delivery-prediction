"""DP1: copy the source systems into bronze for business day ds, then validate bronze.

    ingest_bronze    jobs/dp1_ingest.py --until ds   oms rows changed and export dates up to ds
    validate_bronze  jobs/validate.py --suite dp1    Great Expectations on the slice of day ds;
                                                     fails the run if any expectation fails
    trigger_dp2      starts dp2_transform_validate for the same day, only after validation passed

One run per local day of the period, plus 2024-12-01 for the parcels delivered after midnight.
Runs are not caught up automatically; trigger a day with
`airflow dags trigger dp1_ingest_validate -e <day>`.
"""

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from lakehouse_tasks import DEFAULT_ARGS, PERIOD_END, PERIOD_START, spark_job

with DAG(
    dag_id="dp1_ingest_validate",
    description="Sources (oms, app-exports) to bronze, then Great Expectations on bronze",
    schedule="@daily",
    start_date=PERIOD_START,
    end_date=PERIOD_END,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["dp1", "bronze"],
    doc_md=__doc__,
) as dag:
    ingest = spark_job("ingest_bronze", "dp1_ingest.py", ["--until", "{{ ds }}"])
    validate = spark_job(
        "validate_bronze", "validate.py",
        ["--suite", "dp1", "--ds", "{{ ds }}", "--period-end", "{{ var.value.period_end }}", "--fail-on-error"], retries=0,
    )
    trigger_dp2 = TriggerDagRunOperator(
        task_id="trigger_dp2",
        trigger_dag_id="dp2_transform_validate",
        logical_date="{{ logical_date }}",
        reset_dag_run=True,
    )
    ingest >> validate >> trigger_dp2
