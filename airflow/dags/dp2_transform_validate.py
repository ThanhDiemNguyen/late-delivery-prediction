"""DP2: bronze to silver and gold, gold to the warehouse, then validate silver and gold.

    silver        jobs/dp2_silver.py         clean bronze into silver (merge and new dates)
    gold_dims     jobs/dp2_gold_dims.py      dimensions, SCD2 dim_courier merged
    gold_facts    jobs/dp2_gold_facts.py     fact_delivery merged, new activity hours
    warehouse     jobs/dp2_warehouse.py      the dwh star schema, swapped in one transaction
    validate_suite     jobs/validate.py --suite dp2     Great Expectations on day ds
    validate_scd2      jobs/check_dim_courier.py        the SCD2 invariants of dim_courier
    trigger_dp3        starts dp3_features_validate for the same day, only after both checks passed

Triggered by dp1_ingest_validate for the same day once bronze has passed its suite. Any failed
check fails the run.
"""

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from lakehouse_tasks import DEFAULT_ARGS, EXPORTS, OMS_TABLES, PERIOD_START, lake, postgres, spark_job

BRONZE = lake("bronze", *[f"raw_{t}" for t in OMS_TABLES + EXPORTS])
SILVER = lake("silver", "stg_hubs", "stg_customers", "stg_courier_changes", "stg_orders", "stg_courier_pings", "stg_parcel_scans")
DIMS = lake("gold", "dim_hub", "dim_customer", "dim_date", "dim_zone", "dim_courier")
FACTS = lake("gold", "fact_delivery", "fact_courier_activity_hourly")
DWH = postgres("dwh", "dim_date", "dim_hub", "dim_zone", "dim_customer", "dim_courier", "fact_delivery", "obt_delivery")

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
    silver = spark_job("silver", "dp2_silver.py", [], inlets=BRONZE, outlets=SILVER)
    dims = spark_job(
        "gold_dims", "dp2_gold_dims.py", [],
        inlets=lake("silver", "stg_hubs", "stg_customers", "stg_courier_changes"), outlets=DIMS,
    )
    facts = spark_job(
        "gold_facts", "dp2_gold_facts.py", [],
        inlets=lake("silver", "stg_orders", "stg_courier_pings") + lake("gold", "dim_courier", "dim_hub"), outlets=FACTS,
    )
    warehouse = spark_job(
        "warehouse", "dp2_warehouse.py", [],
        inlets=DIMS + lake("gold", "fact_delivery"), outlets=DWH,
    )
    suite = spark_job(
        "validate_suite", "validate.py",
        ["--suite", "dp2", "--ds", "{{ ds }}", "--period-end", "{{ var.value.period_end }}", "--fail-on-error"], retries=0,
        inlets=SILVER + DIMS + FACTS,
    )
    scd2 = spark_job(
        "validate_scd2", "check_dim_courier.py", [], retries=0,
        inlets=lake("gold", "dim_courier") + lake("silver", "stg_courier_changes"),
    )
    trigger_dp3 = TriggerDagRunOperator(
        task_id="trigger_dp3",
        trigger_dag_id="dp3_features_validate",
        logical_date="{{ logical_date }}",
        reset_dag_run=True,
    )
    silver >> dims >> facts >> warehouse >> [suite, scd2] >> trigger_dp3
