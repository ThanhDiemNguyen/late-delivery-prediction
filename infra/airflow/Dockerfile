# Airflow 2.10.5 (Python 3.10) with a Spark client identical to the cluster's, in two stages on the
# slim Airflow image. The full image ships every provider; the DAGs use the Spark provider, the
# Amazon provider (the S3 hook that reads the validation results) and the DataHub plugin, and the
# metadata database needs the Postgres provider's driver. The first stage builds the drivers'
# Python; the runtime copies it, Java and the parts of /opt/spark that spark-submit needs (conf is
# mounted). Sizes against Dockerfile.naive are in docs/environment.md.
#   Spark client        Java 17 and /opt/spark/{bin,jars,python} of lastleg/spark:3.5.9, so the
#                       drivers Airflow starts (client mode) match the executors exactly
#   /opt/driver-python  the Spark drivers' Python: Great Expectations 0.18, Sedona, h3 and delta-spark,
#                       whose pins clash with Airflow's
#   providers           the versions of the full image: postgres 6.0.0, amazon 9.2.0, openlineage 2.0.0
#                       with openlineage 1.27.0 (the DataHub plugin's listener imports it; without it the
#                       plugin fails to load and no lineage is sent); apache-spark 5.0.0 without its
#                       PySpark dependency; the DataHub listener plugin 1.5.0.17
FROM apache/airflow:slim-2.10.5-python3.10 AS driver-python

USER root
RUN mkdir -p /opt/driver-python && chown airflow:0 /opt/driver-python
USER airflow
# Bytecode caches and the packages' own test suites are not needed at run time.
RUN python -m venv /opt/driver-python \
    && /opt/driver-python/bin/pip install --no-cache-dir great_expectations==0.18.19 apache-sedona==1.9.0 h3==4.5.0 \
    && /opt/driver-python/bin/pip install --no-cache-dir --no-deps delta-spark==3.3.3 importlib_metadata==8.5.0 zipp==3.21.0 \
    && find /opt/driver-python -depth -type d \( -name __pycache__ -o -name tests \) -exec rm -rf {} +

FROM apache/airflow:slim-2.10.5-python3.10

USER root
COPY --from=lastleg/spark:3.5.9 /opt/java/openjdk /opt/java/openjdk
COPY --from=lastleg/spark:3.5.9 /opt/spark/bin /opt/spark/bin
COPY --from=lastleg/spark:3.5.9 /opt/spark/jars /opt/spark/jars
COPY --from=lastleg/spark:3.5.9 /opt/spark/python /opt/spark/python
COPY --from=driver-python --chown=airflow:0 /opt/driver-python /opt/driver-python
ENV JAVA_HOME=/opt/java/openjdk \
    SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:/opt/java/openjdk/bin:$PATH

USER airflow
RUN pip install --no-cache-dir "apache-airflow==2.10.5" "apache-airflow-providers-postgres==6.0.0" \
        "apache-airflow-providers-amazon==9.2.0" "apache-airflow-providers-openlineage==2.0.0" \
        "openlineage-python==1.27.0" "openlineage-integration-common==1.27.0" "openlineage_sql==1.27.0" \
        "acryl-datahub-airflow-plugin==1.5.0.17" \
    && pip install --no-cache-dir --no-deps "apache-airflow-providers-apache-spark==5.0.0"
