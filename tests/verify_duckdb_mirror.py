"""
Confirms the DuckDB local mirror has row-count parity with the source Delta
tables at mirror time. Not a substitute for tests/verify.py's data-quality
checks — this only proves the mirror copied cleanly, nothing about the
data's validity (that's already covered upstream).
"""
import duckdb
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from mirror_to_duckdb import TABLES, DUCKDB_PATH
from databricks.connect import DatabricksSession

def verify():
    spark = DatabricksSession.builder.getOrCreate()
    con = duckdb.connect(str(DUCKDB_PATH))

    print("=== DuckDB Mirror Parity Check ===")
    all_match = True
    for local_name, source_table in TABLES.items():
        local_count = con.execute(f"SELECT COUNT(*) FROM {local_name}").fetchone()[0]
        source_count = spark.table(source_table).count()
        match = local_count == source_count
        all_match &= match
        status = "OK" if match else "MISMATCH"
        print(f"[{status}] {local_name}: local={local_count} source={source_count}")

    con.close()
    if not all_match:
        raise SystemExit("Mirror parity check failed — rerun mirror_to_duckdb.py")
    print("\nAll tables match.")

if __name__ == "__main__":
    verify()