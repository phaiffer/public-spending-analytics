# Architecture

This repository is designed as a local-first data engineering project for Brazilian federal public spending analytics.

The first architecture decision is restraint: keep the MVP batch-oriented, understandable, and runnable on Windows without external services.

## Design Principles

- Use official bulk downloadable files as the primary source.
- Keep raw files immutable once downloaded.
- Convert raw CSV files into typed Parquet datasets before deeper modeling.
- Use DuckDB as the local analytical engine.
- Use dbt for SQL model structure, testing conventions, and documentation.
- Prefer Python entrypoints over Makefile-driven workflows for Windows friendliness.
- Avoid orchestration and cloud infrastructure until the pipeline has a stable analytical core.

## Layers

### Raw Layer

Path: `data/raw/`

Purpose:

- Store manually downloaded source files exactly as received.
- Preserve source file names and folder organization.
- Avoid edits, deduplication, or manual cleaning.

Expected file types:

- CSV files from Portal da Transparencia bulk downloads.
- ZIP files may be stored temporarily if the source provides compressed downloads.

Git behavior:

- Raw data files are ignored.
- Empty folder placeholders are retained.

### Staging File Layer

Path: `data/staging/`

Purpose:

- Store normalized Parquet files created from raw CSV files.
- Apply technical standardization such as column names, encoding handling, date parsing, decimal parsing, and file metadata.
- Avoid business-heavy transformations.

Expected output:

- Parquet datasets partitioned by source, year, month, or another practical source-driven pattern.

### DuckDB Layer

Default local database path:

```text
data/curated/gov_spending.duckdb
```

Purpose:

- Query local Parquet files efficiently.
- Provide the execution engine for dbt via `dbt-duckdb`.
- Keep development simple and reproducible.

### dbt Modeling Layer

Path: `dbt/`

Purpose:

- Define staging SQL models over normalized data.
- Build intermediate models that align spending stages.
- Build marts that answer analytical questions.
- Add tests for accepted values, uniqueness, non-null fields, and amount sanity checks.

Model layers:

- `models/staging/`: source-aligned models with light cleanup.
- `models/intermediate/`: reusable transformations, joins, and standardization.
- `models/marts/`: business-facing fact and dimension models.

## Intended Flow

```text
Manual source download
        |
        v
data/raw/
        |
        v
Python ingestion
        |
        v
CSV parsing and normalization
        |
        v
data/staging/*.parquet
        |
        v
DuckDB external reads or loaded tables
        |
        v
dbt transformations
        |
        v
curated analytical tables
```

## Future Core Model

The project is designed around a future `fact_public_spending` model.

Expected grain will be decided after profiling real files. The preferred target is one row per normalized spending-stage event with document traceability.

Candidate fact columns:

- `spending_document_id`
- `source_file_name`
- `source_extracted_at`
- `fiscal_year`
- `spending_date`
- `government_body_id`
- `beneficiary_id`
- `spending_stage`
- `amount_brl`

Candidate dimensions:

- `dim_date`
- `dim_government_body`
- `dim_beneficiary`
- `dim_spending_stage`

## Why DuckDB

DuckDB is a strong fit for this MVP because it:

- runs locally without a server
- reads Parquet efficiently
- works well on Windows
- integrates with Python and dbt
- is appropriate for portfolio-scale analytical workloads

Trade-off:

- DuckDB is not a distributed processing engine. If the project later expands into very large multi-year processing, the ingestion strategy may need chunking, partitioning, or a different execution engine. That decision is intentionally deferred.

## Why dbt

dbt provides:

- explicit SQL model lineage
- test definitions close to models
- a familiar analytics engineering structure
- a clean way to separate staging, intermediate, and mart transformations

Trade-off:

- dbt is not necessary for every small local project. It is included here because the repository is intended to demonstrate analytical modeling discipline, not just file conversion scripts.

## What Is Deferred

- Automated downloads.
- API ingestion.
- Full schema mapping for all source files.
- Production-grade data quality framework.
- Dashboard layer.
- CI/CD.
- Cloud deployment.
- Workflow orchestration.
