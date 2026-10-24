"""DP3: gold to the offline feature tables, incrementally, then validate them.

    features             jobs/dp3_features.py --incremental   only what the facts' Change Data Feed changed
    validate_suite       jobs/validate.py --suite dp3         Great Expectations on day ds
    validate_incremental jobs/check_cdf.py                    the incremental tables equal a full recompute
    publish_assertions                                        the suite's results to DataHub as assertions, pass or fail
    checks_passed                                             succeeds only when both checks passed

Triggered by dp2_transform_validate for the same day once silver and gold have passed their
checks. Any failed check fails the run: Airflow sets a run's state from its last tasks, and
publish_assertions succeeds even after a failed suite, so checks_passed carries the result.
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datahub_assertions import publish_assertions
from lakehouse_tasks import DEFAULT_ARGS, PERIOD_START, lake, spark_job

GOLD_IN = lake("gold", "fact_delivery", "fact_courier_activity_hourly", "dim_courier", "dim_hub", "dim_zone", "dim_date")
FEATURES = lake("gold", "feat_courier_daily", "feat_hub_hourly", "feat_zone_daily")

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
    features = spark_job("features", "dp3_features.py", ["--incremental"], inlets=GOLD_IN, outlets=FEATURES)
    suite = spark_job(
        "validate_suite", "validate.py",
        ["--suite", "dp3", "--ds", "{{ ds }}", "--period-end", "{{ var.value.period_end }}", "--fail-on-error"], retries=0,
        inlets=FEATURES,
    )
    incremental = spark_job("validate_incremental", "check_cdf.py", [], retries=0, inlets=FEATURES + GOLD_IN)
    features >> [suite, incremental] >> EmptyOperator(task_id="checks_passed")
    suite >> publish_assertions("dp3", "validate_suite")
