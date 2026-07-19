"""
Unit tests on Silver-layer validation logic.

validate.py's validate_records() has no Spark dependency and is tested
directly here with plain Python/pandas input.

transform.py's functions are not tested here. They take and return
Spark DataFrames, and this project's Spark setup (databricks-connect)
only supports remote sessions against the live Databricks warehouse --
there's no local/offline Spark session available to build a test
DataFrame with. Running these tests against the real warehouse would
work, but would be slow, require live cloud auth, and cost compute for
every test run -- the opposite of what a unit test should be.

A separate test-only environment with plain, non-Databricks pyspark
would restore local testing without any code changes, since these
functions already take a DataFrame in and return a DataFrame out. Worth
doing if this transformation logic grows more complex; not worth the
added maintenance for its current size.

Correctness today is covered by tests/verify.py (checks against real
pipeline output) and dbt test (10 passing tests on the Gold marts).
"""
import pandas as pd
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from validate import validate_records


SCHEMA = {
    "type": "object",
    "properties": {
        "symbol": {"type": "string"},
        "close": {"type": "number"},
    },
    "required": ["symbol", "close"],
}


def test_validate_records_flags_malformed_row(tmp_path):
    schema_path = tmp_path / "schema.json"
    schema_path.write_text(__import__("json").dumps(SCHEMA))

    df = pd.DataFrame([
        {"symbol": "AAPL", "close": 150.0},
        {"symbol": "QQQ", "close": "not_a_number"},  # wrong type, triggers ValidationError
    ])

    valid, flagged = validate_records(df, str(schema_path), "test_source")

    assert len(valid) == 1
    assert len(flagged) == 1
    assert flagged[0]["record"]["symbol"] == "QQQ"


def test_validate_records_passes_clean_data(tmp_path):
    schema_path = tmp_path / "schema.json"
    schema_path.write_text(__import__("json").dumps(SCHEMA))

    df = pd.DataFrame([
        {"symbol": "AAPL", "close": 150.0},
        {"symbol": "SPY", "close": 450.0},
    ])

    valid, flagged = validate_records(df, str(schema_path), "test_source")

    assert len(valid) == 2
    assert len(flagged) == 0