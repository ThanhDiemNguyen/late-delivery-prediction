-- Creates the platform's five databases on the first start of an empty Postgres volume.
-- Postgres runs files in /docker-entrypoint-initdb.d only once, when the data folder is empty.
--   oms       the order management system: a source system owned by "Operations"
--   dwh       the warehouse (star schema and obt_ tables) loaded by DP2
--   airflow   Airflow's metadata database
--   metastore the Hive Metastore catalog of lakehouse tables
--   datahub   DataHub's metadata database

CREATE DATABASE oms;
CREATE DATABASE dwh;
CREATE DATABASE airflow;
CREATE DATABASE metastore;
CREATE DATABASE datahub;
