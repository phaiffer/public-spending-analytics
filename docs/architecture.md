# Architecture

This repository is designed as a local-first data engineering project for Brazilian federal public spending analytics.

The first architecture decision is restraint: keep the MVP batch-oriented, understandable, and runnable on Windows without external services.

## Design Principles

- Use official bulk downloadable files as the primary source.
- Keep raw files immutable once downloaded.
- Profile one real source file before locking ingestion or modeling assumptions.
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

### Profiling Layer

Path: `profiling/`

Purpose:

- Inspect one manually downloaded CSV file before ingestion code is finalized.
- Capture actual columns, row count, sample records, inferred basic types, and null-heavy columns.
- Produce local JSON artifacts that can be reviewed and summarized in documentation.

Git behavior:

- Generated profiling JSON files are ignored by default because they may include sampled public records.
- The folder placeholder is retained.

Confirmed implementation:

- The CLI command `gov-spending profile-raw-file` profiles one selected CSV file.
- File selection supports an explicit `--file` path or a `--pattern` match under `data/raw/`.
- Type inference and canonical mapping suggestions are heuristic and sample-based.

Current limitation:

- Real raw files and generated profile artifacts are local-only and gitignored. The first staging implementation reads those local artifacts when present; no official raw records are committed to the repository.

### Staging File Layer

Path: `data/staging/`

Purpose:

- Store normalized Parquet files created from raw CSV files.
- Apply technical standardization such as column names, encoding handling, date parsing, decimal parsing, and file metadata.
- Avoid business-heavy transformations.

Expected output:

- Parquet datasets organized by source system, source family, and source-inferred spending stage.

Confirmed first implementation:

- Command: `gov-spending stage-despesas-file`
- Source family: Portal da Transparencia `Despesas`
- Scope: one selected raw CSV plus its matching profile artifact
- Output: one Parquet file under `data/staging/portal_transparencia/despesas/<spending_stage>/`
- Grain: one row per raw CSV data row
- Source evidence: the command reads the profiling artifact, validates the raw header against profiled columns, and accepts only unambiguous profile-based canonical mappings

Current staging assumptions:

- `amount_brl` must map unambiguously from the profile artifact.
- `spending_document_id` is provisional and optional because real `Despesas` files may expose more than one plausible document identifier.
- `spending_stage` is inferred from the official `Despesas` file name family.
- Source columns are preserved as normalized `source__*` fields.
- Optional canonical fields are present only when the profile artifact supports a single clear mapping.
- The staging write is blocked if required staged columns are missing or null, source traceability fields are missing or null, `source_row_number` is invalid, or `amount_brl` contains negative values.

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
Python source profiling
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

Expected grain will be decided after profiling and staging enough real files. The preferred target remains one row per normalized spending-stage event with document traceability, but this is provisional.

Candidate grain options to evaluate from real profiles:

- one row per document header, useful when the selected source file is header-level
- one row per document item, useful when item files are needed for budget classification detail
- one row per document-beneficiary relationship, useful when payment files include final beneficiary lists
- one row per normalized spending-stage event, useful for unifying commitment, liquidation, and payment analysis

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
- Final fact grain selection.
- Final mart grain selection.
- Commitment-liquidation-payment linkage rules.
- Production-grade data quality framework.
- Dashboard layer.
- CI/CD.
- Cloud deployment.
- Workflow orchestration.
