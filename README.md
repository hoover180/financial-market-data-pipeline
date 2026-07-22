# Financial Market Data Pipeline

![CI](https://github.com/hoover180/financial-market-data-pipeline/actions/workflows/ci.yml/badge.svg)

> ## Business Problem
>
> Institutional investors and quantitative analysts need reliable historical
> market data to support backtesting, factor modeling, and portfolio analytics.
> This pipeline ingests market data from heterogeneous sources, validates data
> quality, models historical dimensions, and publishes analytics-ready datasets
> — emphasizing reliability, reproducibility, and maintainability over raw
> feature count.

## Status

🚧 In active development. See build plan for phase-by-phase progress.

## Architecture

```mermaid
flowchart LR
    subgraph Sources
        YF[yfinance API]
        FRED[FRED API]
    end

    subgraph Bronze[Bronze - done]
        BE[bronze_equities]
        BD[bronze_dim_securities_snapshot]
        BT[bronze_treasury_yields]
    end

    subgraph Silver[Silver - done]
        SE[silver_equities]
        DS[dim_securities - SCD2]
        CL[correction_log]
        ST[silver_treasury_yields]
    end

    subgraph Gold[Gold - done]
        G[dbt marts]
    end

    subgraph Quality[Data Quality - done]
        GE[Great Expectations]
    end

    subgraph Local[Local Dev]
        DDB[(DuckDB mirror)]
    end

    subgraph Orchestration[Orchestration - done]
        FP[financial_pipeline DAG]
        DL[dbt_lineage DAG - Cosmos]
    end

    YF --> BE --> SE
    FRED --> BT --> ST
    YF --> BD --> DS
    SE -.corrections.-> CL
    SE -.-> G
    ST -.-> G
    SE -.checks.-> GE
    ST -.checks.-> GE
    G -.checks.-> GE
    Bronze -.mirror.-> DDB
    Silver -.mirror.-> DDB
    FP -.orchestrates.-> Bronze
    FP -.orchestrates.-> Silver
    FP -.triggers.-> DL
    DL -.orchestrates.-> Gold
```

_Full architecture writeup with engineering decisions coming in Phase 11._

### Local Development

Bronze/Silver/Dimension/Audit tables are mirrored into a local DuckDB file
for dev-loop validation, avoiding serverless compute costs on every query
during active development. Read-only mirror, refreshed on demand via
`src/mirror_to_duckdb.py`.

### Data Quality

Five consumer-facing tables — `silver_equities`, `silver_treasury_yields`,
`fct_daily_returns`, `fct_market_yield_daily`, and `dim_securities_current`
— are validated on demand via Great Expectations, running directly against
Databricks SQL. Coverage is deliberately scoped to freshness and range/set
checks that complement, rather than duplicate, the uniqueness/not-null/
referential-integrity coverage already provided by `dbt test` (10 passing
tests) and `tests/verify.py`.

<p>
<img src="./docs/screenshots/great_expectations_report.png" alt="Great Expectations validation results">
</p>

<p>
<img src="./docs/screenshots/great_expectations_silver_equities_suite.png" alt="Great Expectations suite definition example">
</p>

Full rationale — execution engine choice, table/check scoping — in
`docs/data_modeling_decisions.md`.

### Orchestration

Two Airflow DAGs run in a local Docker Compose stack (Postgres, scheduler,
webserver): `financial_pipeline` (ingest → Silver → dbt trigger, with
retries and an opt-in failure-injection demo) and `dbt_lineage`, an
[astronomer-cosmos](https://astronomer.github.io/astronomer-cosmos/)
`DbtDag` that auto-generates one Airflow task per dbt model from the
project's manifest, running each in an isolated Docker container rather
than inside the Airflow environment itself.

<p>
<img src="./docs/screenshots/dbt_lineage_cosmos.png" alt="Cosmos-generated dbt lineage DAG, all tasks succeeded">
</p>

`financial_pipeline` demonstrates retry-based recovery via an opt-in
Airflow Variable (`PHASE8_INJECT_FAILURE`) that deliberately fails
`transform_silver` on its first attempt, then succeeds on retry:

<p>
<img src="./docs/screenshots/airflow_dag_success.png" alt="financial_pipeline DAG, full clean run, all tasks succeeded">
</p>

<p>
<img src="./docs/screenshots/airflow_dag_recovery.png" alt="transform_silver task audit log showing running, failed, running, success">
</p>

Full rationale — execution mode, manifest-based task loading, runtime
credential handling, headless container auth — in
`docs/data_modeling_decisions.md`.

### Performance & Scaling

Partition pruning, ZORDER, and Liquid Clustering were benchmarked against a
synthetic dataset sized to exercise Delta Lake's storage-layer optimizations
(real Bronze/Silver tables are dev-scale and too small to demonstrate these
patterns). Partitioning by `year_month` reduced bytes read by 97.9% for a
representative date-range query. ZORDER and Liquid Clustering produced
structural null results at this data volume — investigated and explained
using Delta's own operation metrics rather than left unexplained.

Full methodology, results, and screenshots in
`docs/performance_and_scaling.md`.

## Tech Stack

| Tool                             | Purpose                                               |
| -------------------------------- | ----------------------------------------------------- |
| Databricks (PySpark, Delta Lake) | Bronze/Silver/Gold medallion ingestion                |
| dbt-core                         | Staging + Gold marts, tests, docs                     |
| DuckDB                           | Local dev-loop validation                             |
| Great Expectations               | Data quality checks                                   |
| Airflow                          | Orchestration (retries, logging)                      |
| Astronomer Cosmos                | dbt orchestration — auto-generated per-model task DAG |
| Terraform                        | IaC — Unity Catalog schema + grants                   |
| GitHub Actions                   | CI/CD — dbt tests, SQL lint                           |

**Certificates:**

<p>
<a href="https://credentials.databricks.com/5240c3fa-81e4-4f09-b641-c430ad4d795f#acc.vTkrLBYX"><img src="./docs/certs/databricks-badge.png" alt="Databricks Fundamentals" height="90" style="vertical-align: middle;"></a>
<a href="https://credentials.getdbt.com/e5eafe7d-cd32-4dd5-94d1-c395294e34e6#acc.kfzIescG"><img src="./docs/certs/dbt-badge.png" alt="dbt Fundamentals" height="90" style="vertical-align: middle;"></a>
</p>

## Progress Tracker

- [x] Phase 0 — Setup & scaffold
- [x] Phase 1 — Databricks & dbt Fundamentals Certificates
- [x] Phase 2 — Bronze ingestion & data contracts
- [x] Phase 3A — Silver: cleaning
- [x] Phase 3B — Silver: historical dimensions & corrections
- [x] Phase 4 — DuckDB local validation
- [x] Phase 5 — dbt + Gold layer
- [x] Phase 6 — Great Expectations
- [x] Phase 7 — Terraform
- [x] Phase 8 — Airflow orchestration
- [x] Phase 9 — CI/CD
- [x] Phase 10 — Performance & scaling documentation
- [ ] Phase 11 — Documentation & README
- [ ] Phase 12 — Polish & publish
