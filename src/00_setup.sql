-- Databricks notebook source
-- Creates the Unity Catalog objects used by the pipeline.
-- The catalog name comes from the `catalog` job parameter (bundle variable `catalog`).

CREATE CATALOG IF NOT EXISTS IDENTIFIER(:catalog);

-- COMMAND ----------

-- One schema per layer; `raw` holds the landing Volume for the ANP CSV files.
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.raw');

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.bronze');

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.silver');

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.gold');

-- COMMAND ----------

-- Managed Volume where the ingest task lands the raw CSV files.
CREATE VOLUME IF NOT EXISTS IDENTIFIER(:catalog || '.raw.files');
