# Databricks notebook source
# Cleans and types bronze.prices_raw into <catalog>.silver.prices (full overwrite).
# Rules live in src/anp_fuel/silver_rules.py; the quality checks below fail the job on violation.

from functools import reduce
from operator import or_

from pyspark.sql import functions as F

from anp_fuel.silver_rules import DEDUP_KEYS, clean

dbutils.widgets.text("catalog", "anp_fuel")
catalog = dbutils.widgets.get("catalog")

BRONZE = f"{catalog}.bronze.prices_raw"
SILVER = f"{catalog}.silver.prices"

# COMMAND ----------

silver = clean(spark.table(BRONZE)).drop("_ingested_at")
silver.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(SILVER)

# COMMAND ----------

# Quality checks (PRD): sale price between 1 and 15 R$/l, no null keys, silver count <= bronze count.
result = spark.table(SILVER)
key_columns = DEDUP_KEYS + ["state", "city", "region"]
checks = result.agg(
    F.count("*").alias("rows"),
    F.count_if((F.col("sale_price") < 1) | (F.col("sale_price") > 15)).alias("price_out_of_range"),
    F.count_if(reduce(or_, [F.col(c).isNull() for c in key_columns])).alias("null_keys"),
).first()
bronze_rows = spark.table(BRONZE).count()

errors = []
if checks.price_out_of_range:
    errors.append(f"{checks.price_out_of_range} rows with sale_price outside 1-15")
if checks.null_keys:
    errors.append(f"{checks.null_keys} rows with a null key in {key_columns}")
if checks.rows > bronze_rows:
    errors.append(f"silver has more rows ({checks.rows}) than bronze ({bronze_rows})")
if errors:
    raise ValueError("Silver quality checks failed: " + "; ".join(errors))

print(f"{SILVER}: {checks.rows} rows (bronze {bronze_rows}); quality checks passed")
