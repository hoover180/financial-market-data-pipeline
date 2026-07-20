"""
airflow/dags/financial_pipeline_dag.py

Ingest -> Silver -> dbt-trigger orchestration. Mirrors the module
boundaries already established in src/: each task is a thin wrapper
around one existing entry-point function, not new pipeline logic.

Failure-injection/recovery demo: set the Airflow Variable
PHASE8_INJECT_FAILURE to "true" to make transform_silver fail on its
first attempt and succeed on retry -- proves Airflow's retry/recovery
behavior for the airflow_dag_recovery.png screenshot. Leave unset
(or "false") for normal runs and for the airflow_dag_success.png
screenshot.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

log = logging.getLogger(__name__)

default_args = {
    "owner": "michael",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def _extract_validate_load_bronze(**context):
    from extract import run_extraction
    from validate import validate_records
    from load import load_all
    import pandas as pd

    log.info("extract_validate_load_bronze: starting extraction")
    extracted = run_extraction()

    validated = {}
    for table_key, df in extracted.items():
        schema_file = "equities_schema.json" if table_key == "equities" else "treasury_schema.json"
        valid_rows, flagged = validate_records(df, f"config/schemas/{schema_file}", table_key)
        log.info(f"{table_key}: {len(valid_rows)} valid, {len(flagged)} flagged")
        validated[table_key] = pd.DataFrame(valid_rows)

    results = load_all(validated)
    for r in results:
        log.info(f"loaded: {r}")
    return results


def _extract_reference_snapshot(**context):
    from extract_reference import load_equities_symbols, extract_security_snapshot
    from load import get_spark_session, write_bronze

    symbols = load_equities_symbols()
    log.info(f"extract_reference_snapshot: pulling reference data for {symbols}")
    df = extract_security_snapshot(symbols)

    spark = get_spark_session()
    try:
        result = write_bronze(spark, df, "dim_securities_snapshot", min_write_ratio=0.99)
        log.info(f"loaded: {result}")
        return result
    finally:
        spark.stop()


def _transform_silver(**context):
    from load import get_spark_session
    from transform import run_silver_transform

    ti = context["ti"]
    inject_failure = Variable.get("PHASE8_INJECT_FAILURE", default_var="false").lower() == "true"

    if inject_failure and ti.try_number == 1:
        log.warning(
            "PHASE8_INJECT_FAILURE=true and this is attempt 1 -- raising a "
            "simulated failure to demonstrate Airflow retry/recovery."
        )
        raise RuntimeError("Simulated failure for Phase 8 recovery demo (attempt 1)")

    spark = get_spark_session()
    try:
        run_silver_transform(spark)
        log.info("transform_silver: Bronze -> Silver complete")
    finally:
        spark.stop()


def _apply_scd2_dim_securities(**context):
    from load import get_spark_session
    from transform_dim_securities import apply_scd2_dim_securities

    spark = get_spark_session()
    try:
        result = apply_scd2_dim_securities(spark)
        log.info(f"apply_scd2_dim_securities: {result}")
        return result
    finally:
        spark.stop()


def _apply_corrections(**context):
    from load import get_spark_session
    from apply_corrections import detect_and_apply_corrections

    spark = get_spark_session()
    try:
        result = detect_and_apply_corrections(spark, window_days=30)
        log.info(f"apply_corrections: {result}")
        return result
    finally:
        spark.stop()


with DAG(
    dag_id="financial_pipeline",
    description="Ingest (Bronze) -> Silver -> dbt trigger, with retries and an "
                 "opt-in failure-injection demo (PHASE8_INJECT_FAILURE Variable).",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["phase8", "ingest", "silver"],
) as dag:

    extract_validate_load_bronze = PythonOperator(
        task_id="extract_validate_load_bronze",
        python_callable=_extract_validate_load_bronze,
    )

    extract_reference_snapshot = PythonOperator(
        task_id="extract_reference_snapshot",
        python_callable=_extract_reference_snapshot,
    )

    transform_silver = PythonOperator(
        task_id="transform_silver",
        python_callable=_transform_silver,
    )

    apply_scd2_dim_securities = PythonOperator(
        task_id="apply_scd2_dim_securities",
        python_callable=_apply_scd2_dim_securities,
    )

    apply_corrections = PythonOperator(
        task_id="apply_corrections",
        python_callable=_apply_corrections,
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt",
        trigger_dag_id="dbt_lineage",
        wait_for_completion=True,
        poke_interval=15,
        reset_dag_run=True,
    )

    extract_validate_load_bronze >> transform_silver
    extract_reference_snapshot >> apply_scd2_dim_securities
    transform_silver >> apply_corrections
    [apply_scd2_dim_securities, apply_corrections] >> trigger_dbt