# Flink 1.20.5 with PyFlink and the Kafka connector, in two stages. PyPI has no arm64 wheel for
# PyFlink, so the first stage compiles it with the compilers and Python headers; the runtime image
# installs only the built wheels (mounted from that stage, so they are no layer of their own) and
# keeps no compiler. The sizes against Dockerfile.naive are in docs/environment.md.
#   apache-flink 1.20.5               PyFlink, built here from source
#   libpython3.10                     the shared Python library that thread mode (pemja) loads
#   flink-sql-connector-kafka 3.4.0   KafkaSource and KafkaSink for Flink 1.20
#   flink-python in lib/              in thread mode every job loads pemja's native library; from a
#                                     job's own classloader the second job on a TaskManager fails
#                                     (NoSuchFieldException: loadedLibraryNames on Java 17), so pemja
#                                     comes from lib/, parent-first (classloader setting in compose)
FROM flink:1.20.5-scala_2.12-java17 AS wheels

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-dev python3-pip build-essential \
    && pip3 wheel --no-cache-dir --wheel-dir /wheels "apache-flink==1.20.5"

FROM flink:1.20.5-scala_2.12-java17

RUN --mount=type=bind,from=wheels,source=/wheels,target=/wheels \
    apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip libpython3.10 \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && pip3 install --no-cache-dir --no-index --find-links /wheels "apache-flink==1.20.5" \
    && apt-get purge -y python3-pip && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar /opt/flink/lib/
RUN cp /opt/flink/opt/flink-python-1.20.5.jar /opt/flink/lib/
