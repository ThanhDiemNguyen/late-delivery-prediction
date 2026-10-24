"""Publish the Great Expectations results of a validate task to DataHub as assertions.

jobs/validate.py writes s3a://lakehouse/_validation/<suite>/ds=<ds>/results.json. For each expectation
this task sends one assertion on the dataset it checked (its definition, the same in every run) and
one run event with the outcome on day ds. The dataset's Quality tab then shows every check and the
result of every run, failed runs included.
"""

import json
from datetime import datetime

from airflow.configuration import conf
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule
from datahub.emitter.mce_builder import (
    datahub_guid, make_assertion_urn, make_data_platform_urn, make_dataset_urn, make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AssertionInfoClass, AssertionResultClass, AssertionResultTypeClass, AssertionRunEventClass,
    AssertionRunStatusClass, AssertionSourceClass, AssertionSourceTypeClass, AssertionStdAggregationClass as Agg,
    AssertionStdOperatorClass as Op, AssertionStdParameterClass, AssertionStdParametersClass,
    AssertionStdParameterTypeClass, AssertionTypeClass, DataPlatformInstanceClass, DatasetAssertionInfoClass,
    DatasetAssertionScopeClass as Scope, PartitionSpecClass, PartitionTypeClass,
)
from datahub_airflow_plugin.hooks.datahub import DatahubRestHook

BUCKET, PREFIX = "lakehouse", "_validation"
PLATFORM = "great-expectations"

# expectation -> (scope, aggregation, operator); a range operator is set from the kwargs.
# UNIQUE_PROPOTION is the spelling DataHub's GraphQL API knows; UNIQUE_PROPORTION breaks the Quality tab.
STANDARD = {
    "expect_column_values_to_not_be_null": (Scope.DATASET_COLUMN, Agg.IDENTITY, Op.NOT_NULL),
    "expect_column_values_to_be_in_set": (Scope.DATASET_COLUMN, Agg.IDENTITY, Op.IN),
    "expect_column_values_to_be_unique": (Scope.DATASET_COLUMN, Agg.UNIQUE_PROPOTION, Op.EQUAL_TO),
    "expect_column_values_to_be_between": (Scope.DATASET_COLUMN, Agg.IDENTITY, None),
    "expect_column_mean_to_be_between": (Scope.DATASET_COLUMN, Agg.MEAN, None),
    "expect_table_row_count_to_be_between": (Scope.DATASET_ROWS, Agg.ROW_COUNT, None),
    "expect_table_row_count_to_equal": (Scope.DATASET_ROWS, Agg.ROW_COUNT, Op.EQUAL_TO),
    "expect_column_pair_values_A_to_be_greater_than_B": (Scope.DATASET_COLUMN, Agg._NATIVE_, None),
    "expect_compound_columns_to_be_unique": (Scope.DATASET_ROWS, Agg._NATIVE_, Op._NATIVE_),
    "expect_column_to_exist": (Scope.DATASET_SCHEMA, Agg._NATIVE_, Op._NATIVE_),
}


def dataset_urn(table: str) -> str:
    """Return the URN of the lakehouse table a check ran on.

    A derived check such as "gold.fact_delivery labels" belongs to the table its name starts with.
    """
    return make_dataset_urn("trino", f"delta.{table.split()[0]}")


def number(value) -> AssertionStdParameterClass:
    """Return a numeric parameter of an assertion."""
    return AssertionStdParameterClass(value=str(value), type=AssertionStdParameterTypeClass.NUMBER)


def describe(table: str, name: str, kwargs: dict) -> str:
    """Return a one-line description of an expectation, as the Quality tab lists it."""
    low, high = kwargs.get("min_value"), kwargs.get("max_value")
    bounds = f"between {low} and {high}" if low is not None and high is not None else (
        f">= {low}" if low is not None else f"<= {high}")
    text = {
        "expect_column_values_to_not_be_null": f"{kwargs.get('column')} is never null",
        "expect_column_values_to_be_in_set": f"{kwargs.get('column')} takes only allowed values",
        "expect_column_values_to_be_unique": f"{kwargs.get('column')} is unique",
        "expect_column_values_to_be_between": f"{kwargs.get('column')} is {bounds}",
        "expect_column_mean_to_be_between": f"mean of {kwargs.get('column')} is {bounds}",
        "expect_table_row_count_to_be_between": f"row count is {bounds}",
        "expect_table_row_count_to_equal": f"row count is {kwargs.get('value')}",
        "expect_column_pair_values_A_to_be_greater_than_B":
            f"{kwargs.get('column_A')} {'>=' if kwargs.get('or_equal') else '>'} {kwargs.get('column_B')}",
        "expect_compound_columns_to_be_unique": f"({', '.join(kwargs.get('column_list', []))}) is unique",
        "expect_column_to_exist": f"column {kwargs.get('column')} exists",
    }.get(name, name)
    derived = table.partition(" ")[2]
    return f"{derived}: {text}" if derived else text


def assertion_info(table: str, name: str, kwargs: dict) -> AssertionInfoClass:
    """Return the definition of one expectation as a DataHub dataset assertion."""
    urn = dataset_urn(table)
    scope, aggregation, operator = STANDARD.get(name, (Scope.UNKNOWN, Agg._NATIVE_, Op._NATIVE_))
    low, high = kwargs.get("min_value"), kwargs.get("max_value")
    parameters = None
    if operator is None and "column_A" in kwargs:
        operator = Op.GREATER_THAN_OR_EQUAL_TO if kwargs.get("or_equal") else Op.GREATER_THAN
    elif operator is None and low is not None and high is not None:
        operator, parameters = Op.BETWEEN, AssertionStdParametersClass(minValue=number(low), maxValue=number(high))
    elif operator is None:
        operator = Op.GREATER_THAN_OR_EQUAL_TO if low is not None else Op.LESS_THAN_OR_EQUAL_TO
        parameters = AssertionStdParametersClass(value=number(low if low is not None else high))
    elif name == "expect_column_values_to_be_unique":
        parameters = AssertionStdParametersClass(value=number(1))
    elif name == "expect_table_row_count_to_equal":
        parameters = AssertionStdParametersClass(value=number(kwargs["value"]))
    columns = kwargs.get("column_list") or [kwargs[k] for k in ("column", "column_A", "column_B") if k in kwargs]
    return AssertionInfoClass(
        type=AssertionTypeClass.DATASET,
        description=describe(table, name, kwargs),
        source=AssertionSourceClass(type=AssertionSourceTypeClass.EXTERNAL),
        customProperties={"check": table, "expectation": name},
        datasetAssertion=DatasetAssertionInfoClass(
            dataset=urn, scope=scope, aggregation=aggregation, operator=operator, parameters=parameters,
            fields=[make_schema_field_urn(urn, c) for c in columns] or None,
            nativeType=name, nativeParameters={k: json.dumps(v) for k, v in kwargs.items()},
        ),
    )


def assertion_mcps(report: dict) -> list[MetadataChangeProposalWrapper]:
    """Return the assertions of one validation report and the run event of each, for day ds."""
    run_at = datetime.fromisoformat(report["run_at"])
    run_id = f"{report['suite']}-{report['ds']}-{report['run_at']}"
    platform = DataPlatformInstanceClass(platform=make_data_platform_urn(PLATFORM))
    mcps = []
    for r in report["results"]:
        key = {"platform": PLATFORM, "suite": report["suite"], "check": r["table"], "expectation": r["expectation"], "kwargs": r["kwargs"]}
        urn = make_assertion_urn(datahub_guid(key))
        observed = r["observed"]
        value = observed.get("observed_value")
        result = AssertionResultClass(
            type=AssertionResultTypeClass.SUCCESS if r["success"] else AssertionResultTypeClass.FAILURE,
            rowCount=observed.get("element_count"),
            unexpectedCount=observed.get("unexpected_count"),
            actualAggValue=float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None,
            nativeResults={k: json.dumps(v) for k, v in observed.items()},
        )
        mcps += [
            MetadataChangeProposalWrapper(entityUrn=urn, aspect=assertion_info(r["table"], r["expectation"], r["kwargs"])),
            MetadataChangeProposalWrapper(entityUrn=urn, aspect=platform),
            MetadataChangeProposalWrapper(entityUrn=urn, aspect=AssertionRunEventClass(
                timestampMillis=int(run_at.timestamp() * 1000), runId=run_id, assertionUrn=urn,
                asserteeUrn=dataset_urn(r["table"]), status=AssertionRunStatusClass.COMPLETE, result=result,
                partitionSpec=PartitionSpecClass(type=PartitionTypeClass.PARTITION, partition=f"ds={report['ds']}"),
            )),
        ]
    return mcps


def read_report(suite: str, ds: str) -> dict | None:
    """Return the report validate.py wrote for the suite and day, or None if there is none."""
    s3 = S3Hook(aws_conn_id="lakehouse_s3")
    keys = s3.list_keys(bucket_name=BUCKET, prefix=f"{PREFIX}/{suite}/ds={ds}/results.json/part-") or []
    return json.loads(s3.read_key(keys[0], bucket_name=BUCKET)) if keys else None


def publish(suite: str, validate_task_id: str, ds: str, dag_run, **_) -> None:
    """Send the report of this run's validate task to DataHub."""
    if not conf.getboolean("datahub", "enabled", fallback=False):
        raise AirflowSkipException("the DataHub plugin is off (DATAHUB_ENABLED=false)")
    report = read_report(suite, ds)
    validated = dag_run.get_task_instance(validate_task_id).start_date
    if report is None or validated is None or datetime.fromisoformat(report["run_at"]) < validated:
        raise AirflowSkipException(f"{validate_task_id} wrote no results in this run")
    mcps = assertion_mcps(report)
    DatahubRestHook("datahub_rest_default").make_emitter().emit_mcps(mcps)
    print(f"suite {suite} ds {ds}: {len(report['results'])} assertions sent to DataHub, "
          f"{report['failed']} failed; {len(mcps)} aspects")


def publish_assertions(suite: str, validate_task_id: str) -> PythonOperator:
    """Return the task that publishes a suite's results, whether its validate task passed or failed."""
    return PythonOperator(
        task_id="publish_assertions",
        python_callable=publish,
        op_kwargs={"suite": suite, "validate_task_id": validate_task_id},
        trigger_rule=TriggerRule.ALL_DONE,
        retries=1,
    )
