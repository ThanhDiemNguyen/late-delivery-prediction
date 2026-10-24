"""DP3: gold to the offline feature tables, incrementally, then validate them.

    features             jobs/dp3_features.py --incremental   only what the facts' Change Data Feed changed
    validate_suite       jobs/validate.py --suite dp3         Great Expectations on day ds
    validate_incremental jobs/check_cdf.py                    the incremental tables equal a full recompute

Triggered by dp2_transform_validate for the same day once silver and gold have passed their
checks. Any failed check fails the run.
"""

from airflow import DAG
from lakehouse_tasks import DEFAULT_ARGS, PERIOD_START, spark_job

with DAG(
    dag_id="dp3_features_validate",
    description="Gold to feat_ tables from the Change Data Feed; then Great Expectations and the full-recompute check",
    schedule=None,
    start_date=PERIOD_START,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["dp3", "features"],
    doc_md=__doc__,
) as dag:
    features = spark_job("features", "dp3_features.py", ["--incremental"])
    suite = spark_job(
        "validate_suite", "validate.py",
        ["--suite", "dp3", "--ds", "{{ ds }}", "--period-end", "{{ var.value.period_end }}", "--fail-on-error"], retries=0,
    )
    incremental = spark_job("validate_incremental", "check_cdf.py", [], retries=0)
    features >> [suite, incremental]
