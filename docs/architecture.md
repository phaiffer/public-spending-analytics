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

- Store source files exactly as received, whether downloaded by hand or by `gov-spending sync-recebimentos`.
- Preserve source file names and folder organization.
- Avoid edits, deduplication, or manual cleaning.

Expected file types:

- CSV files from Portal da Transparencia bulk downloads.
- JSON files from narrow Portal da Transparencia API extractions.
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

### API Raw Ingestion Layer

Path: `data/raw/portal_transparencia_api/despesas/`

Purpose:

- Extract a narrow, filtered despesas document slice from the official Portal da Transparencia API.
- Persist raw JSON responses faithfully before any normalization decisions.
- Capture a manifest with request parameters, timestamps, page count, record count, source endpoint, and raw page files.

Confirmed first implementation:

- Command: `gov-spending ingest-despesas-documentos`
- Endpoint: `GET /api-de-dados/despesas/documentos`
- Supported slice: one `dataEmissao`, one numeric `fase`, and at least one extra filter
- Example: `--data-emissao 02/01/2025 --fase 3 --unidade-gestora <VALUE>`
- API key source: environment variable configured by `sources.portal_transparencia_api.api_key_env_var`
- Pagination: page-by-page requests stop when an empty JSON list is returned

Current API assumptions:

- This endpoint is constrained to a one-day request and should not be used as a broad backfill mechanism.
- Live extraction requires valid filter values such as `unidadeGestora` or `gestao`.
- Raw JSON fields are not treated as stable canonical fields until a live response is reviewed.
- The API path is for practical first extraction and filtered subsets; bulk CSV remains the preferred route for large source coverage.

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

Confirmed `Recebimentos de Recursos por Favorecido` implementation:

- Commands: `gov-spending sync-recebimentos` stages every month that has new content; `gov-spending stage-recebimentos-favorecido-file` stages the reviewed January 2026 file by hand; `gov-spending restage-recebimentos` rebuilds every month from the archives already on disk
- Source family: `recebimentos_recursos_por_favorecido`
- Scope: every monthly archive the Portal publishes, from `2013-12` to the current month
- Output: one Parquet file per month under `data/staging/portal_transparencia/recebimentos_recursos_por_favorecido/`, read by dbt through a glob
- Grain: one row per raw CSV row
- Row-count check: each staged file carries `source_registered_row_count`, and dbt compares it with the rows it reads from that file
- Amount type: `amount_received_brl` is always written as `DECIMAL(18,4)`, so every monthly file shares one schema
- Source evidence: profiles confirm `latin-1`, semicolon delimiter, and `12` columns; the reviewed January 2026 version has `300350` rows (an earlier unhashed version had `300391`)
- `Ano e mês do lançamento` is parsed as a monthly key, not a daily timestamp
- `Valor Recebido` is parsed into `amount_received_brl`
- Negative `amount_received_brl` values are allowed because they are present in the full staged file

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

Confirmed first real dbt model:

- Source: `portal_transparencia_staging_files.recebimentos_recursos_por_favorecido`
- Source registration: external DuckDB Parquet source over the staged file written by Python
- Model: `stg_portal_transparencia__recebimentos_recursos_por_favorecido`
- Model grain: one row per staged row, which is one row per raw CSV row
- Amount handling: `amount_received_brl` remains signed
- Beneficiary handling: `beneficiary_id` remains text
- Geography handling: location fields remain source-aligned and can include values such as `EX`
- Month handling: `launch_month` is preserved, with `launch_month_key`, `launch_month_start_date`, `launch_year`, and `launch_month_number` added for analytical use

Confirmed first intermediate analytical model:

- Model: `int_recebimentos_recursos_por_favorecido_monthly`
- Materialization: a table, so the marts and data tests downstream read the aggregated full history once instead of re-aggregating `120` million staged rows each time; the dbt profile caps DuckDB at 6 GB and lets it spill to disk
- Grain: one row per month, beneficiary, government body, management unit, beneficiary location code, and beneficiary municipality name
- Inputs: the real dbt staging model for `Recebimentos de Recursos por Favorecido`
- Measures: `total_amount_received_brl`, `record_count`, and `negative_amount_record_count`
- Amount handling: signed amounts are preserved and aggregated without forcing non-negative values
- Traceability: aggregate rows retain source row-number bounds and source file name bounds for navigation back to staging
- Source-row uniqueness: `source_row_number` should be treated as unique within `source_file_name`, not as a future global key across every file

Confirmed first mart-ready model:

- Model: `mart_recebimentos_by_beneficiary_month`
- Grain: one row per month, `beneficiary_id`, and `beneficiary_name`
- Measures: `total_amount_received_brl`, `total_record_count`, and `negative_amount_record_count`
- Grain decision: `beneficiary_name` remains part of the grain because the real source contains multiple names for the same `beneficiary_id`; all such IDs are masked CPFs (`beneficiary_identifier_kind = 'cpf_masked'`)
- Scope decision: organizational and geographic attributes are treated as descriptive upstream context, not as part of the first mart grain
- Traceability: mart rows retain source file-name bounds and source row-number bounds

Confirmed monthly summary mart:

- Model: `mart_recebimentos_monthly_summary`
- Grain: one row per month, superior government body id and name, and `beneficiary_identifier_kind`
- Measures: `total_amount_received_brl`, `total_record_count`, and `negative_amount_record_count`
- Consumer: the Streamlit dashboard, which reads it through short-lived read-only DuckDB connections

Identifier kinds and Treasury codes:

- `beneficiary_identifier_kind` is set in staging from the shape of `beneficiary_id`; `RB` codes are looked up in the reviewed seed `treasury_bank_codes` and become `treasury_operation` (cash management inside the government) or `treasury_payment` (aggregated payments such as INSS benefits and payroll); `TN` codes become `intergovernmental_transfer`
- An `RB` code missing from the seed is staged as `treasury_unclassified`, and `assert_treasury_bank_codes_are_classified` warns with its code and name
- The dashboard leaves `treasury_operation` out of its totals by default

## Intended Flow

```text
gov-spending sync-recebimentos (daily timer) or manual source download
        |
        v
data/raw/  +  data/sync/ (versions by SHA-256)
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
        |
        v
Streamlit dashboard (gov-spending dashboard, read-only)
```

Recebimentos runs this flow per month: one staged Parquet per month, read by dbt
through a glob, rebuilt whenever the sync job stages a new content version.

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

## What Is In Place

- Automated downloads for `Recebimentos`: `gov-spending sync-recebimentos`, scheduled by systemd user timers (`scripts/install_sync_timer.sh`, Linux only).
- A local Streamlit dashboard: `gov-spending dashboard`.
- CI on GitHub Actions: lint, tests, and `dbt build` on a synthetic sample for Python 3.11 to 3.13.

## What Is Deferred

- Broad API ingestion.
- Automated downloads for source families other than `Recebimentos`.
- Full schema mapping for all source files.
- Final fact grain selection.
- Commitment-liquidation-payment linkage rules.
- Production-grade data quality framework.
- Hosting the dashboard publicly.
- Cloud deployment.
- Workflow orchestration beyond the systemd timers.
