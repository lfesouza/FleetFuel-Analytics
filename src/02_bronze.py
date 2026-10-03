# Databricks notebook source
# Loads the raw ANP CSVs from the Volume into <catalog>.bronze.prices_raw.
# All columns are kept as strings; only files not yet in the table are appended (idempotent).

import csv
import os
import re
import unicodedata

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "anp_fuel")
catalog = dbutils.widgets.get("catalog")

VOLUME_DIR = f"/Volumes/{catalog}/raw/files"
TABLE = f"{catalog}.bronze.prices_raw"

# COMMAND ----------


def to_snake_case(name: str) -> str:
    """'Regiao - Sigla' -> 'regiao_sigla'; strips the UTF-8 BOM and accents so names are valid Delta columns."""
    name = unicodedata.normalize("NFKD", name.replace("﻿", "")).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def count_data_rows(path: str) -> int:
    """Rows after the header that have at least one non-empty field (ANP files end with ';;;' filler rows)."""
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        next(reader)
        return sum(1 for row in reader if any(field.strip() for field in row))


all_files = sorted(
    os.path.join(root, name)
    for root, _, names in os.walk(VOLUME_DIR)
    for name in names
    if name.endswith(".csv")
)

loaded = set()
if spark.catalog.tableExists(TABLE):
    loaded = {r._source_file for r in spark.table(TABLE).select("_source_file").distinct().collect()}

new_files = [p for p in all_files if p not in loaded]
print(f"{len(all_files)} files in Volume, {len(loaded)} already loaded, {len(new_files)} new")

# COMMAND ----------

if new_files:
    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .option("encoding", "UTF-8")
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "FAILFAST")
        .csv(new_files)
    )
    data_cols = [to_snake_case(c) for c in raw.columns]
    df = (
        raw.toDF(*data_cols)
        .dropna(how="all", subset=data_cols)
        .withColumn("_source_file", F.regexp_replace(F.col("_metadata.file_path"), "^dbfs:", ""))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    df.write.mode("append").saveAsTable(TABLE)

# COMMAND ----------

# Quality check: every new file must land with exactly as many rows as its CSV has data rows.
if new_files:
    actual = {
        r._source_file: r["count"]
        for r in spark.table(TABLE).where(F.col("_source_file").isin(new_files)).groupBy("_source_file").count().collect()
    }
    expected = {p: count_data_rows(p) for p in new_files}
    mismatches = {p: (expected[p], actual.get(p, 0)) for p in new_files if expected[p] != actual.get(p, 0)}
    if mismatches:
        raise ValueError(f"Row count mismatch (csv, bronze): {mismatches}")
    print(f"Row counts match for {len(new_files)} files: {sum(actual.values())} rows appended")

print(f"{TABLE} total rows: {spark.table(TABLE).count()}")
