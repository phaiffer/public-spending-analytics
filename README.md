# Brazilian Federal Public Spending Analytics

Standalone portfolio repository for building a local-first analytical foundation over Brazilian federal public spending data from official public sources.

This project focuses on the federal spending lifecycle, especially commitment, liquidation, and payment stages, analyzed by government body and beneficiary. It is designed as a serious data engineering portfolio project: clear scope, reproducible local execution, documented assumptions, and an extensible path from raw public files to analytical marts.

## Why This Project Matters

Brazilian federal public spending data is publicly available, but it is not always easy to use analytically. The data is large, operationally shaped, and spread across files that require careful interpretation before business questions can be answered.

This repository aims to turn official bulk data into a transparent analytical model that can answer practical questions such as:

- Which government bodies committed, liquidated, and paid the most spending?
- Which beneficiaries received the largest payments?
- How do spending stages compare across time?
- Where are there gaps between committed, liquidated, and paid amounts?
- Which spending patterns deserve deeper public scrutiny?

The goal is not to build a dashboard first. The goal is to build the data foundation that would make trustworthy dashboards, notebooks, and public analyses possible later.

## Scope

### In Scope For The MVP

- Brazilian federal public spending data.
- Official bulk downloadable files from Portal da Transparencia.
- Batch-oriented ingestion from local CSV files.
- Local analytical processing with DuckDB.
- Parquet as the intermediate storage format.
- dbt models prepared for staging, intermediate, and mart layers.
- A future core fact table for spending amounts by date, government body, beneficiary, and spending stage.

### Out Of Scope For The MVP

- API-first ingestion.
- State and municipal spending.
- Procurement, contracts, agreements, and transfers as the main analytical scope.
- Dashboards or BI tools.
- Cloud infrastructure.
- CI/CD.
- Airflow, Prefect, Spark, Kubernetes, Terraform, or other heavy platform tooling.

These areas may become future phases after the federal spending model is stable.

## MVP Definition

The MVP is a local reproducible pipeline foundation that can eventually:

1. Accept downloaded Portal da Transparencia public spending CSV files in `data/raw/`.
2. Normalize selected spending files into clean Parquet datasets under `data/staging/`.
3. Load and transform those datasets with DuckDB and dbt.
4. Produce curated analytical tables under `data/curated/` or a local DuckDB database.
5. Support spending analysis by:
   - date
   - government body
   - beneficiary
   - spending stage
   - amount

The current repository foundation intentionally does not download real data automatically.

The first implemented data path is deliberately narrow: one manually downloaded and profiled Portal da Transparencia `Despesas` CSV can be staged locally as Parquet after its profiling artifact confirms the source header and unambiguous required mappings.

## Target Users

- **Data engineering recruiters and reviewers** who want to see practical modeling, documentation, and local data pipeline design.
- **Public policy analysts** who need clear spending metrics before building analysis.
- **Civic tech practitioners** interested in reproducible use of official open data.
- **The repository owner** as a focused standalone project separate from a broader portfolio.

## Key Analytical Questions

- What is the total committed, liquidated, and paid spending by month?
- Which government bodies have the highest paid amounts?
- Which beneficiaries receive the largest payments?
- Where are the largest differences between committed and paid amounts?
- How do payment patterns change across fiscal years?
- Which spending stages are available at document level, and which require aggregation rules?

## Main KPIs

- **Committed Amount**: Total value formally committed by the government.
- **Liquidated Amount**: Total value recognized as delivered or payable after liquidation.
- **Paid Amount**: Total value effectively paid.
- **Payment Rate**: Paid amount divided by committed amount, where the denominator is valid.
- **Liquidation Rate**: Liquidated amount divided by committed amount, where the denominator is valid.
- **Open Commitment Amount**: Committed amount minus paid amount.
- **Beneficiary Concentration**: Share of paid amount represented by top beneficiaries.

Metric details and caveats are documented in [docs/metric_definitions.md](docs/metric_definitions.md).

## High-Level Architecture

```text
Official bulk CSV downloads
        |
        v
data/raw/
        |
        v
Source profiling
        |
        v
Python ingestion and normalization
        |
        v
data/staging/ as Parquet
        |
        v
DuckDB local analytical database
        |
        v
dbt models
  - staging
  - intermediate
  - marts
        |
        v
data/curated/ and analytical tables
```

The architecture is intentionally simple. Python handles file discovery, configuration, and normalization tasks. DuckDB provides fast local analytics. dbt provides SQL modeling structure, tests, and documentation conventions.

## Intended Analytical Model

The MVP is designed around a future `fact_public_spending` table.

Likely fact grain:

- one row per normalized spending event or spending document line, depending on source file availability and deduplication rules
- associated spending stage: commitment, liquidation, or payment
- amount in Brazilian reais
- document identifiers retained for traceability

Likely dimensions:

- `dim_date`
- `dim_government_body`
- `dim_beneficiary`
- `dim_spending_stage`

The exact grain will be finalized after profiling real downloaded files.

## Roadmap

### Phase 1: Repository Foundation

- Define project identity, scope, and architecture.
- Create Python package structure.
- Prepare dbt + DuckDB project structure.
- Document sources, metrics, and assumptions.
- Create local data folder conventions.

### Phase 2: Ingestion MVP

- Download a small sample period manually from Portal da Transparencia.
- Implement local CSV discovery and schema inspection.
- Normalize selected spending files into Parquet.
- Add basic data quality checks.

### Phase 3: dbt Modeling MVP

- Build staging models for selected spending files.
- Create intermediate models for spending-stage unification.
- Create the first spending mart.
- Add dbt tests for keys, accepted values, and amount sanity checks.

### Phase 4: Analytical Outputs

- Add reproducible example queries.
- Add portfolio-ready analysis notebooks or static reports.
- Document findings and limitations.

### Phase 5: Expansion

- Add more years and source file types.
- Consider contracts, procurement, transfers, or subnational data.
- Evaluate dashboarding only after the model is stable.

## What This Repository Proves

From a data engineering perspective, this repository is intended to demonstrate:

- pragmatic source selection from official public data
- local-first batch pipeline design
- clear separation of raw, staging, and curated data
- analytical modeling with fact and dimension thinking
- DuckDB usage for local analytics
- dbt project organization without premature platform complexity
- metric definition discipline
- honest documentation of assumptions and deferred work

## Getting Started

This foundation does not require real data to install.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
```

Check the CLI placeholder:

```powershell
gov-spending --help
```

Create a local DuckDB database placeholder:

```powershell
gov-spending bootstrap-duckdb
```

## Source Profiling Workflow

This repository does not download Portal da Transparencia files automatically. Download one official spending CSV manually and keep the original file name.

Recommended Windows folder pattern:

```powershell
New-Item -ItemType Directory -Force -Path data\raw\portal_transparencia\despesas\2025\01
```

Place one downloaded file under that folder, for example:

```text
data/raw/portal_transparencia/despesas/2025/01/20250101_Despesas_Pagamento.csv
```

List discovered raw CSV files:

```powershell
gov-spending list-raw-files
```

Profile one selected file:

```powershell
gov-spending profile-raw-file --file data\raw\portal_transparencia\despesas\2025\01\20250101_Despesas_Pagamento.csv
```

Or select by a filename substring when exactly one file matches:

```powershell
gov-spending profile-raw-file --pattern Pagamento
```

The profiling command writes a JSON summary under `profiling/` by default. The output includes:

- detected encoding and delimiter
- row count
- source columns
- normalized column names
- sample records
- sample-based type inference
- null-heavy columns
- heuristic canonical column suggestions

Profiling outputs may include sampled public records, so generated JSON files are ignored by git by default. Review them before deciding whether to publish a summarized version.

## First Staging Workflow

The first staging command is limited to one Portal da Transparencia `Despesas` CSV and its matching profile artifact. It validates that the raw CSV header still matches the profiled columns before writing Parquet.

Run profiling first:

```powershell
gov-spending profile-raw-file --file data\raw\portal_transparencia\despesas\2025\01\20250101_Despesas_Pagamento.csv
```

Then stage that same file:

```powershell
gov-spending stage-despesas-file --file data\raw\portal_transparencia\despesas\2025\01\20250101_Despesas_Pagamento.csv
```

If the profile artifact is stored somewhere else, pass it explicitly:

```powershell
gov-spending stage-despesas-file `
  --file data\raw\portal_transparencia\despesas\2025\01\20250101_Despesas_Pagamento.csv `
  --profile profiling\20250101_Despesas_Pagamento_profile.json
```

Default output pattern:

```text
data/staging/portal_transparencia/despesas/<spending_stage>/<source file stem>.parquet
```

Current staging grain:

- one staged row per raw CSV data row from the selected official file
- original source columns are preserved with `source__` prefixes after ASCII snake-case normalization
- source metadata is retained, including source file name, profile artifact name, source row number, source family, and spending stage
- canonical fields are populated only when the profile artifact produced one unambiguous source-column mapping

The command currently requires an unambiguous profile-based mapping for:

- `amount_brl`

The command may also populate optional canonical fields when they are unambiguous in the profile:

- `spending_document_id`
- `spending_date`
- `fiscal_year`
- `government_body_id`
- `government_body_name`
- `beneficiary_id`
- `beneficiary_name`

`spending_document_id` is provisional at the staging layer. Real Portal da Transparencia
`Despesas` files can expose more than one plausible document identifier, so staging keeps
all observed source columns and only emits this canonical field when the profile has one
clear source column. `amount_brl` remains required because the first staged spending path
is not useful without a parsed monetary value.

Before writing Parquet, staging now runs lightweight checks tied to the observed profile
and staged output:

- required staged columns exist and are populated
- `amount_brl` values are non-negative
- source traceability fields exist and are populated
- `source_row_number` remains positive

The spending stage is inferred from the official `Despesas` file name family, such as `Despesas_Empenho`, `Despesas_Liquidacao`, or `Despesas_Pagamento`.

## Official Source Starting Point

The initial source strategy is based on the Portal da Transparencia open data download area, especially the public spending files listed under "Despesas publicas", including "Documentos de empenho, liquidacao e pagamento" and "Execucao da despesa". See [docs/source_catalog.md](docs/source_catalog.md) for details and source links checked during repository creation.

Current profiling status in git-tracked documentation: no real raw CSV file or profile artifact is committed because raw data and local profiles are ignored. The staging implementation reads the ignored local profile artifact and uses only columns present in that artifact's `columns` list. Run `gov-spending profile-raw-file` after placing a manually downloaded file under `data/raw/`, then run `gov-spending stage-despesas-file` against the same file.

## License

No license has been selected yet. Add one before publishing or reusing this repository broadly.
