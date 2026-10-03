# Databricks notebook source
# Runs the pytest suite in this folder on serverless compute; the task fails if any test fails.

# COMMAND ----------

# MAGIC %pip install pytest --quiet

# COMMAND ----------

import contextlib
import io
import os
import sys

import pytest

# Workspace files are read-only for bytecode/cache writes, so keep pytest from writing them.
sys.dont_write_bytecode = True
tests_dir = os.getcwd()  # the notebook's own folder (tests/)

report = io.StringIO()
with contextlib.redirect_stdout(report):
    exit_code = pytest.main([tests_dir, "-v", "--color=no", "-p", "no:cacheprovider"])
print(report.getvalue())
if exit_code != 0:
    raise Exception(f"pytest failed with exit code {exit_code}\n{report.getvalue()[-3000:]}")

# Exposes the report through `databricks jobs get-run-output`.
dbutils.notebook.exit(report.getvalue()[-3000:])
