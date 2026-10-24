"""Shared start-up for every Spark job: a session that is known to use the platform's settings."""

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
