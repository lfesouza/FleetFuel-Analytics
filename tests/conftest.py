import os
import sys

import pytest
from pyspark.sql import SparkSession

# Make src/ importable so tests can `from anp_fuel.silver_rules import ...`.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))


@pytest.fixture(scope="session")
def spark():
    # On Databricks serverless this returns the notebook's active session.
    return SparkSession.builder.getOrCreate()
