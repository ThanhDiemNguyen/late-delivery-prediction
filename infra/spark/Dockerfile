# Spark 3.5.9 with the lakehouse jars, in two stages. The first stage downloads the jars and installs
# the Python packages; the runtime image copies each set in one layer, already with its final
# permissions. Dockerfile.naive downloads the jars with ADD and then runs chmod on them, which stores
# every jar twice. The sizes are compared in docs/environment.md.
#   delta-spark + delta-storage 3.3.3   Delta Lake tables (delta-spark 3.3.x supports Spark 3.5)
#   hadoop-aws 3.3.4 + AWS SDK 1.12.262 S3A access to SeaweedFS (matches Spark's Hadoop 3.3.4)
#   postgresql 42.7.7                   JDBC reads of oms and writes to dwh
#   sedona 1.9.0 + geotools-wrapper     H3 cells and spatial SQL (novel idea 1)
#   great_expectations 0.18.19          the validate stage of DP1 to DP3 (the course's version)
#   apache-sedona 1.9.0, h3 4.5.0       Sedona's Python API, and h3 to check its cells
FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu AS fetch

USER root
WORKDIR /stage
RUN mkdir jars && cd jars \
    && for url in \
         https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.3.3/delta-spark_2.12-3.3.3.jar \
         https://repo1.maven.org/maven2/io/delta/delta-storage/3.3.3/delta-storage-3.3.3.jar \
         https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
         https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
         https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.7/postgresql-42.7.7.jar \
         https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.5_2.12/1.9.0/sedona-spark-shaded-3.5_2.12-1.9.0.jar \
         https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/1.9.0-33.5/geotools-wrapper-1.9.0-33.5.jar; \
       do wget -q "$url"; done \
    && chmod 644 *.jar
# The delta Python API without its PySpark dependency (PySpark comes from SPARK_HOME), then the rest.
# Bytecode caches and the packages' own test suites are not needed at run time.
RUN pip3 install --no-cache-dir --target=/stage/site --no-deps delta-spark==3.3.3 importlib_metadata==8.5.0 zipp==3.21.0 \
    && pip3 install --no-cache-dir --target=/stage/site great_expectations==0.18.19 apache-sedona==1.9.0 h3==4.5.0 \
    && find /stage/site -depth -type d \( -name __pycache__ -o -name tests \) -exec rm -rf {} +

FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu

USER root
COPY --from=fetch /stage/jars/ /opt/spark/jars/
COPY --from=fetch /stage/site/ /usr/local/lib/python3.10/dist-packages/
RUN mkdir -p /opt/spark/events && chown spark:spark /opt/spark/events
USER spark
