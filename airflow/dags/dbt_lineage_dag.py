from datetime import datetime

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.constants import ExecutionMode

import os

profile_config = ProfileConfig(
    profile_name="financial_market_data_pipeline",
    target_name="dev",
    profiles_yml_filepath="/root/.dbt/profiles.yml",
)

project_config = ProjectConfig(
    dbt_project_path="/opt/airflow/dbt/financial_market_data_pipeline",
    manifest_path="/opt/airflow/dbt/financial_market_data_pipeline/target/manifest.json",
)

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.DOCKER,
)

dbt_lineage_dag = DbtDag(
    project_config=project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    operator_args={
        "image": "financial-pipeline-dbt:latest",
        "docker_url": "unix://var/run/docker.sock",
        "network_mode": "bridge",
        "auto_remove": "success",
        "environment": {
            "DBT_DATABRICKS_TOKEN": os.environ.get("DBT_DATABRICKS_TOKEN", ""),
        },
    },
    dag_id="dbt_lineage",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)