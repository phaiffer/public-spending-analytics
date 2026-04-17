# Source Catalog

This catalog documents the intended official data sources for the MVP. It distinguishes confirmed source-page information from observations that require a manually downloaded file.

Source information was checked from Portal da Transparencia public pages during repository foundation work on 2026-04-16.

## Primary Source

### Portal da Transparencia - Dados Abertos

- Publisher: Controladoria-Geral da Uniao / Portal da Transparencia do Governo Federal
- Page: <https://portaldatransparencia.gov.br/download-de-dados>
- Format described by portal: CSV files
- MVP relevance: official bulk downloads for federal public spending

The open data page lists public spending files under "Despesas publicas" with daily update cadence for the following relevant datasets:

- Documentos de empenho, liquidacao e pagamento
- Execucao da despesa
- Recursos transferidos
- Recebimento de recursos por favorecido

For the MVP, this repository prioritizes spending execution and spending-stage documents. Transfers and other related datasets are potential future extensions, not the initial center of gravity.

## MVP Candidate Dataset 1

### Documentos de empenho, liquidacao e pagamento

- Page: <https://portaldatransparencia.gov.br/download-de-dados/despesas>
- Topic: federal spending documents by stage
- Update cadence shown on source catalog page: daily
- Expected role in project: document-level source for commitment, liquidation, and payment stages

The source page describes downloadable files such as:

- `AAAAMMDD_Despesas_Empenho.csv`
- `AAAAMMDD_Despesas_ItemEmpenho.csv`
- `AAAAMMDD_Despesas_Liquidacao.csv`
- `AAAAMMDD_Despesas_Liquidacao_EmpenhosImpactados.csv`
- `AAAAMMDD_Despesas_Pagamento.csv`
- `AAAAMMDD_Despesas_Pagamento_EmpenhosImpactados.csv`
- `AAAAMMDD_Despesas_Pagamento_FavorecidosFinais.csv`
- payment list support files for banks, invoices, and precatorios

Initial use:

- Start with a narrow subset of files needed to represent commitment, liquidation, and payment.
- Profile columns and keys before finalizing the fact table grain.
- Preserve original document identifiers for traceability.

Known caveats:

- Payment documents may include final beneficiaries and lists that require separate modeling decisions.
- Liquidation and payment files may reference impacted commitments, so the final relationship model should not assume a simple one-to-one document flow.
- File sizes can be large; ingestion should support chunked reads in later implementation.

### Current Profiling And Staging Status

Real `Despesas` raw files and generated profile JSON files are local-only and ignored by git. In this checkout, no local official CSV or profile artifact was visible under `data/raw/` or `profiling/` during the staging implementation update, so this document does not claim specific observed source columns.

Confirmed from implementation:

- The raw-data folder exists and is gitignored for real source files.
- The profiling command can discover and profile one manually downloaded CSV file.
- The first staging command reads a profile artifact, validates the raw CSV header against the profiled `columns` list, and writes one Parquet file.
- The first staging command is limited to the Portal da Transparencia `Despesas` source family.
- The first staging grain is one staged row per raw CSV data row from the selected file.
- Source columns are preserved as normalized `source__*` columns for traceability.
- Canonical fields are populated only from profile-based unambiguous mappings.

Required unambiguous staging mappings:

- `spending_document_id`
- `amount_brl`

Optional unambiguous staging mappings:

- `spending_date`
- `fiscal_year`
- `government_body_id`
- `government_body_name`
- `beneficiary_id`
- `beneficiary_name`

Still provisional:

- actual source columns for the local official file
- source column data types beyond profile inference
- row count for the selected local file
- null-heavy fields for the selected local file
- reliable document keys beyond source-row traceability
- relationship between header, item, impacted commitment, payment, and final beneficiary files
- final mart grain
- whether the dbt spending-document scaffold should be enabled for this staged Parquet output

After running `gov-spending profile-raw-file`, summarize the observed columns here before treating a source mapping as confirmed.

Suggested observed-column documentation pattern:

```text
Profiled file: <file name>
Profile artifact: profiling/<file stem>_profile.json
Row count: <row count from profile>
Column count: <column count from profile>
Observed columns:
- <source column> -> <normalized column> -> <candidate canonical field or none>
```

## MVP Candidate Dataset 2

### Execucao da despesa

- Page: <https://portaldatransparencia.gov.br/download-de-dados>
- Topic: public spending execution
- Update cadence shown on source catalog page: daily
- Expected role in project: aggregate or execution-level comparison source

Initial use:

- Evaluate after document-level files are profiled.
- Use as a reconciliation or analytical complement if grain and definitions align.

Known caveats:

- This dataset may be shaped differently from document-stage files.
- Metric definitions must avoid mixing incompatible grains.

## Source Acquisition Policy

For the MVP:

1. Download files manually from official Portal da Transparencia pages.
2. Store raw downloads under `data/raw/`.
3. Do not commit raw files to git.
4. Document the download date, selected filters, and source page in future metadata files.
5. Avoid API-first design until the bulk-file model is working.
6. Run local profiling before committing schema-specific ingestion logic.

## Raw File Naming Guidance

Keep original source file names when possible.

Recommended local folder pattern:

```text
data/raw/portal_transparencia/despesas/YYYY/MM/
```

Example:

```text
data/raw/portal_transparencia/despesas/2025/01/20250101_Despesas_Pagamento.csv
```

This pattern is guidance only. The ingestion code should eventually read source configuration rather than assume every file will arrive perfectly organized.

## Canonical Column Mapping Approach

Column mapping is intentionally two-step:

1. Normalize source labels to ASCII `snake_case`.
2. Suggest candidate canonical fields from normalized labels.
3. For staging, accept only canonical fields with exactly one candidate in the profile artifact.

Current canonical candidates:

- `spending_document_id`
- `spending_date`
- `fiscal_year`
- `government_body_id`
- `government_body_name`
- `beneficiary_id`
- `beneficiary_name`
- `amount_brl`

The profiler writes these suggestions under `canonical_column_suggestions`. These suggestions are not authoritative; staging uses them only when they are unambiguous and still validates the current raw CSV header against the profiled header before writing Parquet.

The `spending_stage` field is expected to come from the file context or source-specific transformation, not necessarily from a raw column.

## First Staging Output

Command:

```powershell
gov-spending stage-despesas-file --file data\raw\portal_transparencia\despesas\YYYY\MM\<official Despesas file>.csv
```

Default output:

```text
data/staging/portal_transparencia/despesas/<spending_stage>/<official file stem>.parquet
```

Current assumptions:

- The raw file has already been profiled with `gov-spending profile-raw-file`.
- The profile artifact defaults to `profiling/<official file stem>_profile.json`.
- The source family is inferred from a `despesas` path component or an official `*_Despesas_*.csv` file name.
- Spending stage is inferred from `Despesas_Empenho`, `Despesas_ItemEmpenho`, `Despesas_Liquidacao`, or `Despesas_Pagamento` in the file name.
- This is a technical staging output, not the final analytical mart.

## Future Sources

Potential future phases may add:

- contracts
- procurement
- transfers
- parliamentary amendments
- subnational spending
- beneficiary registries

These are intentionally deferred so the first version can build a credible spending-stage model.
