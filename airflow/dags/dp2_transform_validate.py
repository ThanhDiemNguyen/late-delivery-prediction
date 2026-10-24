"""DP2: bronze to silver and gold, gold to the warehouse, then validate silver and gold.

    silver        jobs/dp2_silver.py         clean bronze into silver (merge and new dates)
    gold_dims     jobs/dp2_gold_dims.py      dimensions, SCD2 dim_courier merged
    gold_facts    jobs/dp2_gold_facts.py     fact_delivery merged, new activity hours
    warehouse     jobs/dp2_warehouse.py      the dwh star schema, swapped in one transaction
    validate_suite     jobs/validate.py --suite dp2     Great Expectations on day ds
    validate_scd2      jobs/check_dim_courier.py        the SCD2 invariants of dim_courier

Triggered by dp1_ingest_validate for the same day once bronze has passed its suite. Any failed
check fails the run.
"""

from airflow import DAG
from lakehouse_tasks import DEFAULT_ARGS, PERIOD_START, spark_job

with DAG(
    dag_id="dp2_transform_validate",
    description="Bronze to silver, gold and the warehouse; then Great Expectations and SCD2 checks",
    schedule=None,
    start_date=PERIOD_START,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["dp2", "silver", "gold", "dwh"],
    doc_md=__doc__,
) as dag:
    silver = spark_job("silver", "dp2_silver.py", [])
    dims = spark_job("gold_dims", "dp2_gold_dims.py", [])
    facts = spark_job("gold_facts", "dp2_gold_facts.py", [])
    warehouse = spark_job("warehouse", "dp2_warehouse.py", [])
    suite = spark_job(
        "validate_suite", "validate.py",
        ["--suite", "dp2", "--ds", "{{ ds }}", "--period-end", "{{ var.value.period_end }}", "--fail-on-error"], retries=0,
    )
    scd2 = spark_job("validate_scd2", "check_dim_courier.py", [], retries=0)
    silver >> dims >> facts >> warehouse >> [suite, scd2]
