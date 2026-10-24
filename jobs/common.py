"""Shared start-up for every Spark job: a session that is known to use the platform's settings.

stage_metrics reads the job's own stages back from the Spark UI's REST API, so a benchmark can
report task times, shuffle and spill next to its result.
"""

import json
from urllib.request import urlopen

from pyspark.sql import SparkSession

REQUIRED = ("spark.hadoop.fs.s3a.endpoint", "spark.hadoop.hive.metastore.uris", "spark.sql.extensions")


def lakehouse_session(app_name: str) -> SparkSession:
    """Return a SparkSession, or raise if spark-defaults.conf was not loaded.

    Without that file, S3A falls back to Amazon's public endpoint and Delta and the metastore are
    missing. That happens when the mounted infra/spark/conf folder is replaced under a running
    container (for example by switching git branches); recreate the Spark containers then.
    """
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    missing = [key for key in REQUIRED if not spark.conf.get(key, None)]
    if missing:
        spark.stop()
        raise RuntimeError(
            f"Spark settings missing: {missing}. spark-defaults.conf was not loaded; run "
            "docker compose --profile core --profile batch up -d --force-recreate spark-master spark-worker spark-history"
        )
    return spark


def with_sedona(spark: SparkSession) -> SparkSession:
    """Register Apache Sedona's spatial and H3 SQL functions (ST_*) on the session and return it."""
    from sedona.spark import SedonaContext

    return SedonaContext.create(spark)


def stage_metrics(spark: SparkSession) -> list[dict]:
    """Return every completed stage of this application with its task and shuffle metrics.

    Each stage has its task count, run time, input, shuffle and spill bytes, and the median and the
    slowest task (run time and shuffle read), which show whether one task did far more than the rest.
    """
    base = f"{spark.sparkContext.uiWebUrl}/api/v1/applications/{spark.sparkContext.applicationId}"
    report = []
    for stage in sorted(json.load(urlopen(f"{base}/stages?status=complete")), key=lambda s: s["stageId"]):
        summary = json.load(urlopen(f"{base}/stages/{stage['stageId']}/{stage['attemptId']}/taskSummary?quantiles=0.5,1.0"))
        read = summary.get("shuffleReadMetrics", {})
        report.append({
            "stage": stage["stageId"],
            "name": stage["name"][:70],
            "tasks": stage["numTasks"],
            "executor_run_s": round(stage["executorRunTime"] / 1000, 1),
            "input_gb": round(stage["inputBytes"] / 1e9, 3),
            "shuffle_read_gb": round(stage["shuffleReadBytes"] / 1e9, 3),
            "shuffle_write_gb": round(stage["shuffleWriteBytes"] / 1e9, 3),
            "spill_memory_gb": round(stage["memoryBytesSpilled"] / 1e9, 3),
            "spill_disk_gb": round(stage["diskBytesSpilled"] / 1e9, 3),
            "task_run_s_median_max": [round(v / 1000, 1) for v in summary["executorRunTime"]],
            "task_shuffle_read_mb_median_max": [round(v / 1e6, 1) for v in (read.get("readBytes") or [0, 0])],
        })
    return report
