# Source Catalog

This catalog documents the intended official data sources for the MVP. It does not claim that any real files have already been downloaded or validated in this repository.

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

## Future Sources

Potential future phases may add:

- contracts
- procurement
- transfers
- parliamentary amendments
- subnational spending
- beneficiary registries

These are intentionally deferred so the first version can build a credible spending-stage model.
