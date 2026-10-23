# Brazilian Federal Public Spending Analytics

[![CI](https://github.com/phaiffer/public-spending-analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/phaiffer/public-spending-analytics/actions/workflows/ci.yml)

Standalone portfolio repository for building a local-first analytical foundation over Brazilian federal public spending data from official public sources.

This repository is currently centered on one real implemented analytical case
study from the Portal da Transparencia:

- `Recebimentos de Recursos por Favorecido`

It is designed as a serious local-first data engineering portfolio project:
clear scope, reproducible execution, documented trade-offs, and an honest line
between what is already implemented and what is still planned.

## Case Study Summary

This repository is currently a portfolio case study for one real implemented
source family:

- `Recebimentos de Recursos por Favorecido`

What is real in the current pipeline:

- every monthly archive the Portal publishes, `2013-12` through `2026-10`
  (`155` months), synced, checked against its CSV row count, and staged to
  one Parquet file per month
- one dbt staging view, one intermediate model, and two mart models over the
  full history, built incrementally by month
- `dbt build` runs end to end through the dbt CLI: `85` nodes (models, seeds,
  and data tests), all passing; on a desktop with DuckDB capped at 6 GB a full
  refresh takes about three and a half hours and a build with no changed
  month about 15 minutes
- a local Streamlit dashboard over the marts (see [Dashboard](#dashboard))

Full-history facts (data synced on 2026-10-23; `2026-10` is still open):

| Fact | Value |
| --- | ---: |
| Source rows staged | `120093295` |
| Mart rows at beneficiary-month grain | `103576729` |
| Rows with a negative signed amount | `1935627` |
| Distinct beneficiary IDs | `1674906` |
| Masked CPFs with more than one name in the same month | `872852` |
| Net signed total, internal Treasury flows left out | R$ 27.64 trillion |
| Net signed total, every identifier kind | R$ 14.46 trillion |

The two totals differ because the Portal also publishes money that stays inside
the Treasury. Treasury cash management (public debt service, bond issuance,
Treasury and INSS transfers) appears under `RB` codes and nets
R$ -18.40 trillion; payments to the Treasury's own management units, such as
the CGTES cash coordination, net R$ 5.22 trillion. The pipeline classifies both
and leaves them out of the default totals (see [Treasury Codes](#treasury-codes)).

The January 2026 file remains the reviewed reference: it is pinned by SHA-256
in `config/source_manifest.toml` at `300350` rows. The pipeline screenshots and
the Excel samples below come from that single-month run.

## Why This Project Matters

Brazilian federal public spending data is publicly available, but it is not always easy to use analytically. The data is large, operationally shaped, and spread across files that require careful interpretation before business questions can be answered.

This repository aims to turn official public data into transparent analytical
models without pretending the source is cleaner than it is.

The current implemented case study asks a narrower but real question:

- how can one official monthly source file about received resources be profiled,
  staged, tested, and modeled into a conservative beneficiary-month mart?

The broader repository direction is still larger than the implemented case
study:

- a future analytical foundation over the federal spending lifecycle, including
  commitment, liquidation, and payment stages

The goal is not to build a dashboard first. The goal is to build a trustworthy
data foundation and show the engineering judgment behind it.

## Business Problem

The business problem is simple to state and awkward to solve well:

How do you turn official public spending files into something analytical without
pretending the source is cleaner or more stable than it really is?

For the implemented source family, the friction is not just technical file
parsing. It is modeling discipline:

- identifiers may look numeric but should stay textual
- geography includes values such as `EX`, so a Brazil-only assumption would be wrong
- monthly fields should not be inflated into fake daily dates
- signed negative amounts are present in the real file and must not be erased
- beneficiary identifiers do not behave like a clean master key in the current data

That is why this repository favors a conservative pipeline: preserve the raw
meaning first, then add only the analytical structure that is justified by real
source evidence.

## Scope

### Current Implemented Scope

- One real Portal da Transparencia bulk CSV source:
  `Recebimentos de Recursos por Favorecido`
- Local profiling of that real source file
- Python raw-to-staging transformation into Parquet
- DuckDB + dbt staging, intermediate, and first mart-ready modeling for that
  source family
- A targeted raw API ingestion path for the constrained
  `despesas/documentos` endpoint, implemented as raw ingestion only

### Future Planned Scope

- Broader Brazilian federal public spending analytics
- Additional official bulk files from Portal da Transparencia
- Spending-lifecycle modeling across commitment, liquidation, and payment
- A future core fact table for spending amounts by date, government body,
  beneficiary, and spending stage

### Out Of Scope For The MVP

- Broad API-first ingestion across all Portal da Transparencia sources.
- State and municipal spending.
- Procurement, contracts, agreements, and transfers as the main analytical scope.
- Dashboards or BI tools.
- Cloud infrastructure.
- CI/CD.
- Airflow, Prefect, Spark, Kubernetes, Terraform, or other heavy platform tooling.

These areas may become future phases after the federal spending model is stable.

## Current Source Scope

The repository contains groundwork for more than one source family, but the
implemented analytical case study is intentionally narrower.

Current implemented path:

```text
202601_RecebimentosRecursosPorFavorecido.csv
  -> profiling JSON
  -> staged Parquet
  -> dbt staging model
  -> dbt intermediate monthly model
  -> mart_recebimentos_by_beneficiary_month
```

Other source paths in the repository, including the targeted `despesas`
endpoint work, are still partial or intentionally scaffolded compared with this
recebimentos path.

## Repository Direction

The repository has two distinct layers of ambition:

1. Current implemented case study:
   a real end-to-end analytical path for one official `recebimentos` source
2. Future repository direction:
   a broader local analytical foundation for federal spending data

The repository does not currently implement the full broader vision. The
implemented work is intentionally narrower and should be read that way.

## Current Implemented Questions

- What is the signed total amount received per beneficiary in the implemented
  month?
- Which beneficiaries have the largest positive or negative received totals?
- How many source records contribute to each beneficiary-month row?
- How often do negative signed values occur?
- How often does the same `beneficiary_id` appear with more than one
  `beneficiary_name`?

## Future Planned Questions

- Which government bodies committed, liquidated, and paid the most spending?
- Which beneficiaries received the largest payments across spending stages?
- How do spending stages compare across time?
- Where are there gaps between committed, liquidated, and paid amounts?
- Which spending patterns deserve deeper public scrutiny across the broader
  lifecycle?

## Target Users

- **Data engineering recruiters and reviewers** who want to see practical modeling, documentation, and local data pipeline design.
- **Public policy analysts** who need clear spending metrics before building analysis.
- **Civic tech practitioners** interested in reproducible use of official open data.
- **The repository owner** as a focused standalone project separate from a broader portfolio.

## Current Implemented KPIs

- **Total Amount Received**: Signed sum of `amount_received_brl` in the current
  source family.
- **Total Record Count**: Number of source-derived records represented in an
  aggregate row.
- **Negative Amount Record Count**: Number of source-derived records with signed
  negative values.
- **Beneficiary Name Collision Count**: Count of beneficiary IDs that map to
  more than one beneficiary name in the implemented source month.

## Future Planned KPIs

- **Committed Amount**: Total value formally committed by the government.
- **Liquidated Amount**: Total value recognized as delivered or payable after liquidation.
- **Paid Amount**: Total value effectively paid.
- **Payment Rate**: Paid amount divided by committed amount, where the denominator is valid.
- **Liquidation Rate**: Liquidated amount divided by committed amount, where the denominator is valid.
- **Open Commitment Amount**: Committed amount minus paid amount.
- **Beneficiary Concentration**: Share of paid amount represented by top beneficiaries.

Metric details and caveats, including future planned definitions, are
documented in [docs/metric_definitions.md](docs/metric_definitions.md).

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

## Implemented Flow

For the current case study, the implemented architectural flow is:

```text
Official CSV
  202601_RecebimentosRecursosPorFavorecido.csv
        |
        v
Profiling artifact
  profiling/202601_RecebimentosRecursosPorFavorecido_profile.json
        |
        v
Python raw-to-staging transformation
        |
        v
Staged Parquet
  data/staging/portal_transparencia/recebimentos_recursos_por_favorecido/
        |
        v
DuckDB external source in dbt
        |
        v
dbt staging model
  stg_portal_transparencia__recebimentos_recursos_por_favorecido
        |
        v
dbt intermediate model
  int_recebimentos_recursos_por_favorecido_monthly
        |
        v
dbt mart
  mart_recebimentos_by_beneficiary_month
```

That is the implemented pipeline today. It is deliberately local-first and does
not depend on orchestration, cloud services, or dashboard infrastructure.

## Pipeline Evidence

Staging command success

<img src="docs/assets/screenshots/04-staging-command-success.png" alt="Staging command success" width="900" />

DuckDB validation over the staged Parquet

<img src="docs/assets/screenshots/06-duckdb-parquet-validation.png" alt="DuckDB validation over the staged Parquet" width="900" />

Mart-layer SQL preview

<img src="docs/assets/screenshots/07-mart-sql-preview.png" alt="Mart-layer SQL preview" width="900" />

The repository includes evidence across the raw-source, profiling, staged
Parquet, and mart SQL layers, with the strongest pipeline checkpoints shown
here. These screenshots were captured on the earlier `300391`-row version of
the source file.

`dbt build` result against the registered `300350`-row version (2026-10-21):

```text
Finished running 1 seed, 2 table models, 43 data tests, 2 view models
Done. PASS=48 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=48
```

`dbt build --full-refresh` result over the full history, `155` months (2026-10-23):

```text
Finished running 3 incremental models, 3 seeds, 1 table model, 77 data tests, 1 view model in 3 hours 28 minutes and 1.86 seconds (12481.86s).
Done. PASS=85 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=85
```

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

## Modeling Layers

The implemented `recebimentos` path currently has four meaningful layers:

1. Raw:
   the original official CSV, preserved exactly as downloaded
2. Staging:
   one row per raw CSV row, typed and traceable, with no business-heavy reshaping
3. Intermediate:
   a conservative monthly aggregation that still keeps organizational and geographic detail
4. Mart:
   a beneficiary-month table intended for analysis, not for source reconstruction

This shape is deliberate. It lets the repository show analytical progress
without collapsing unresolved source issues too early.

## Key Trade-Offs

The most important trade-offs are not flashy. They are the ones that stop the
model from lying.

`beneficiary_id` remains text:
- the real source presents identifier-looking values that should not be coerced into integers
- the project does not assume the field is always a CNPJ

Geography remains flexible:
- `beneficiary_location_code` is preserved as provided
- values such as `EX` are treated as valid source evidence, not data quality failures

Negative amounts are preserved:
- the full history contains `1935627` negative signed amount rows
- forcing a non-negative rule here would destroy observed source behavior

`beneficiary_name` remains in the mart grain:
- over the full history, `872852` beneficiary IDs map to more than one beneficiary name within a month
- every one of them is a masked CPF, which the Portal publishes with only six digits visible
- collapsing the mart to month + `beneficiary_id` would merge different people into one row

`source_row_number` is only scoped within `source_file_name`:
- a data test checks it is unique per file across every staged month
- it is not advertised as a global key across files and months

## Negative Amounts

Negative amounts are not treated as noise in this repository.

In the implemented `Recebimentos de Recursos por Favorecido` history:

- `1935627` of `120093295` rows contain negative signed amounts, in every
  year of the history
- netted per beneficiary and month, almost all of the negative value sits
  under Treasury cash operations (`treasury_operation`): about
  R$ 42.6 trillion against R$ 24.2 trillion positive, mostly bond issuance
  receipts and redemptions of Treasury investments; CNPJ beneficiaries net
  about R$ 0.02 trillion of negatives

The pipeline therefore keeps `amount_received_brl` signed through staging,
intermediate, and mart layers. The current implementation does not claim a full
business interpretation for every negative value. It only makes the narrower
and safer claim that the source contains them and the analytical model should
preserve them.

## Why `beneficiary_name` Stays In The Mart Grain

This is the most important modeling choice in the current case study.

The first mart is:

```text
mart_recebimentos_by_beneficiary_month
```

Its grain is one row per:

- `launch_month_key`
- `beneficiary_id`
- `beneficiary_name`

`beneficiary_name` is not kept there for decoration. It is part of the grain
because the real data shows that `beneficiary_id` alone is not stable enough
for a beneficiary-level mart. In the implemented file, multiple names can
appear under the same beneficiary ID.

The cause is confirmed over the full history: all `872852` IDs with more than
one name within a month are masked CPFs such as `***.123.456-**`. The Portal
hides the first three and last two digits of individual taxpayers' CPFs, so
different people can share one masked value. No CNPJ (14-digit) identifier has
more than one name within a month. The masked value plus the name is the best
identity the public source offers for individuals, and it is still not a
guaranteed unique person.

Across months the picture differs: `4186` CNPJs carry a different name in a
later month, which reads as companies renaming over twelve years, not as
collisions. The mart grain is per month, so those renames never split a row.

## Beneficiary Identifier Kinds

`beneficiary_identifier_kind` is derived in the dbt staging model from the
shape of `beneficiary_id`, plus two reviewed seeds, `treasury_bank_codes` for
`RB` codes and `treasury_management_units` for Treasury management units,
and carried through the intermediate and mart layers. It does not
change any grain, because it depends only on `beneficiary_id`. Counts below
cover the full history, `2013-12` to `2026-10`:

| Kind | Rule | Rows | Distinct IDs | Multi-name IDs within a month | IDs renamed across months | Net (R$ trillion) |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `cpf_masked` | `***.NNN.NNN-**` | 90653083 | 998531 | 872852 | 990102 | 0.12 |
| `cnpj` | 14 digits | 28312085 | 600504 | 0 | 4186 | 9.06 |
| `foreign_code` | `EX` + 7 characters | 478908 | 67246 | 0 | 111 | 0.24 |
| `management_unit` | other 6-digit SIAFI management unit codes | 356937 | 3905 | 0 | 152 | 1.50 |
| `placeholder` | negative codes: `-1` and `-8` SEM INFORMACAO, `-9` NAO SE APLICA, `-11` Sigiloso, `-13` FOLHA DE PAGAMENTO | 236050 | 5 | 0 | 0 | 0.31 |
| `treasury_payment` | `RB` + 7 digits, classified in the seed as a payment leaving the government | 26305 | 26 | 0 | 0 | 12.01 |
| `pf_code` | `PF` or `PS` + 7 characters: people published without a CPF, aggregate grants, and grant reversals | 9725 | 2670 | 0 | 15 | 0.04 |
| `unregistered_code` | 10 to 12 digits, published only as SEM INFORMACAO or CODIGO INEXISTENTE NO SIAFI | 9218 | 1970 | 0 | 0 | 0.00 |
| `treasury_operation` | `RB` + 7 digits, classified in the seed as Treasury cash management | 5931 | 18 | 0 | 0 | -18.40 |
| `treasury_management_unit` | 6-digit Treasury management unit listed in the seed | 2667 | 8 | 0 | 0 | 5.22 |
| `intergovernmental_transfer` | `TN` + 7 digits | 2386 | 23 | 0 | 0 | 4.35 |
| `treasury_unclassified` | `RB` + 7 digits missing from the seed | 0 | 0 | 0 | 0 | 0 |
| `other_code` | any other shape; the dbt test `assert_beneficiary_ids_have_a_known_shape` warns with each one | 0 | 0 | 0 | 0 | 0 |

`placeholder` IDs are not beneficiaries; they label rows where the beneficiary
is unknown, not applicable, confidential, or a payroll aggregate. Exclude them
from beneficiary rankings. `-8` appears in only `8` months of the history.

The other codes follow the source's own conventions:

- `management_unit` codes are federal management units receiving money from
  other units, such as universities, hospitals, and regional offices. They are
  transfers inside the government, but each one is a real spending unit, so
  the dashboard keeps them in its default totals.
- `pf_code` codes stand for people published without a CPF and for grants
  paid in aggregate (scholarships, student aid). None is located abroad. The
  `6` `PS` codes hold only reversals of the same grants, R$ -6858 in all.
- `unregistered_code` codes are never named: `1855` carry SEM INFORMACAO
  (R$ 32.55 million) and `115` carry CODIGO INEXISTENTE NO SIAFI
  (R$ 0.66 million).

## Treasury Codes

The Portal publishes some Treasury operations with a code in place of the
beneficiary. They are few rows and most of the money:

- `RB` codes are Treasury bank operations. Each of the `44` codes seen in the
  history is classified from its published name in
  [`dbt/seeds/treasury_bank_codes.csv`](dbt/seeds/treasury_bank_codes.csv):
  - `treasury_operation` (`18` codes): cash management inside the government,
    such as `RB0000003` PAGAMENTO DA DIVIDA INTERNA (R$ +18.4 trillion),
    `RB0000011` RECEBIMENTO DE EMISSAO DE TITULOS (R$ -17.9 trillion),
    `RB0000026` RESGATE DE APLIC. DISPONIB. TN (R$ -18.1 trillion), and the
    transfers between the Treasury and the INSS
  - `treasury_payment` (`26` codes): payments that leave the government in
    aggregate, such as `RB0000004` PAGAMENTO DE BENEFICIOS - INSS/FRGPS
    (R$ 9.0 trillion), `RB0038166` FOLHA DE PAGAMENTO (R$ 1.5 trillion),
    `RB0000104` CAIXA ECONOMICA FEDERAL - AG.OPERADORA PBF (Bolsa Familia,
    R$ 0.8 trillion), tax refunds, and contributions to international funds
- `TN` codes (`intergovernmental_transfer`) are transfers to states and
  municipalities: FPM and FPE, oil royalties, Lei Kandir, CIDE.
- Treasury management units (`treasury_management_unit`) are the `8`
  Secretaria do Tesouro Nacional units listed in
  [`dbt/seeds/treasury_management_units.csv`](dbt/seeds/treasury_management_units.csv).
  `170500` COORDENACAO-GERAL DE TESOURARIA - CGTES nets R$ 4.27 trillion,
  mostly paid by the Treasury's own intergovernmental transfers unit
  (R$ 2.92 trillion), by the STN itself (R$ 0.75 trillion), and by the ANP
  and other Minas e Energia units (R$ 0.61 trillion); `170600` COORD.GERAL DE
  CONTROLE DA DIVIDA PUBLICA nets R$ 0.79 trillion, almost all paid by itself.
  Other federal management units are `management_unit`.

An `RB` code missing from the seed is staged as `treasury_unclassified`, and
the dbt test `assert_treasury_bank_codes_are_classified` warns with its code and
name so it can be reviewed. The classification follows the published names; it
is a reviewed reading of the source, not an official budget category.

The dashboard leaves `treasury_operation` and `treasury_management_unit` out of
its totals by default; the viewer can select them.

The dbt test `assert_mart_recebimentos_multi_name_ids_are_masked_cpf` warns if
any non-`cpf_masked` identifier shows a name collision within a month; it
passes over the full history.

## Key Findings

Observations from the full synced history, `2013-12` to `2026-10`:

- **Flows inside the Treasury hide the totals.** The net signed total over
  every identifier kind is R$ 14.46 trillion. Leaving out the `18` Treasury
  cash-operation codes, `5931` rows that net R$ -18.40 trillion, moves it to
  R$ 32.86 trillion; bond issuance and redemptions of Treasury investments are
  published as negative receipts. Leaving out the `8` Treasury management
  units too, `2667` rows that net R$ 5.22 trillion, brings it to
  R$ 27.64 trillion.
- **A handful of codes carry the money.** The `26` Treasury payment codes and
  `23` `TN` codes, `28691` rows out of 120 million, net R$ 16.36 trillion:
  INSS benefits, payroll, Bolsa Familia, and transfers to states and
  municipalities. Named CNPJ beneficiaries net R$ 9.06 trillion.
- **Beneficiary identity is unstable only for individuals.** Within a month,
  `872852` masked CPFs carry more than one name, and no other identifier kind
  carries any. Across months, `4186` CNPJs change name, which matches company
  renames, not collisions.
- **Volume is steady, value grows.** Every full year stages 8.2 to 10.4
  million rows. Without the internal Treasury flows, the net total grows every
  year but one, from R$ 1.41 trillion (2014) to R$ 3.17 trillion (2025); 2020
  (R$ 2.29 trillion) is above 2021 (R$ 2.07 trillion).
- **Social security leads the bodies.** Without the internal Treasury flows,
  the Ministério da Previdência Social nets R$ 9.84 trillion and the
  Ministério da Fazenda R$ 6.01 trillion, ahead of Saúde (R$ 2.07 trillion)
  and Educação (R$ 2.04 trillion).
- **The source format drifts.** From `2023-10` to `2024-03`, `1418` rows carry
  the sentinel superior body `-1` and body `-3` with an empty body name,
  together R$ 1.61 billion. Staging accepts an empty name only behind a
  sentinel code.
- **Placeholders are material.** The five placeholder IDs hold `236050` rows
  and R$ 312 billion net; `-1` SEM INFORMACAO alone nets R$ 160 billion.
- **Negative amounts are routine.** `1935627` rows are negative, in every year.

These findings describe what the source contains. They do not interpret the
budget behind it. Even without the internal Treasury flows, the totals still
include transfers between other federal management units under `management_unit`
(see [Limitations And Future Work](#limitations-and-future-work)).

Rows and net totals by year (2013 holds only December, 2026 ends at the open
month of October):

| Year | Months | Rows | Net, internal Treasury flows left out (R$ billion) | Net, every kind (R$ billion) | Negative rows |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2013 | 1 | 688 | 0.0 | 0.7 | 605 |
| 2014 | 12 | 10427371 | 1407.6 | 546.5 | 357244 |
| 2015 | 12 | 9528128 | 1420.2 | 575.2 | 154563 |
| 2016 | 12 | 9788245 | 1497.6 | 756.0 | 211451 |
| 2017 | 12 | 9851812 | 1591.6 | 856.1 | 136974 |
| 2018 | 12 | 9769474 | 1630.9 | 983.8 | 121151 |
| 2019 | 12 | 10158304 | 1795.2 | 1111.4 | 128617 |
| 2020 | 12 | 8407210 | 2286.6 | 786.3 | 256493 |
| 2021 | 12 | 9319620 | 2065.6 | 688.0 | 59670 |
| 2022 | 12 | 9539865 | 2348.9 | 1123.5 | 176039 |
| 2023 | 12 | 9844439 | 2621.6 | 1448.3 | 165732 |
| 2024 | 12 | 9237291 | 2920.0 | 1952.5 | 78602 |
| 2025 | 12 | 8228227 | 3172.7 | 2014.3 | 54402 |
| 2026 | 10 | 5992621 | 2879.6 | 1614.7 | 34084 |

## Analytical Examples

The current mart supports straightforward monthly beneficiary analysis. Example
queries below assume the mart model has been built in DuckDB/dbt.

Top beneficiaries by signed monthly amount, excluding placeholder IDs:

```sql
select
    launch_month_key,
    beneficiary_id,
    beneficiary_identifier_kind,
    beneficiary_name,
    total_amount_received_brl
from mart_recebimentos_by_beneficiary_month
where beneficiary_identifier_kind != 'placeholder'
order by total_amount_received_brl desc
limit 20;
```

Beneficiaries with the most negative records:

```sql
select
    launch_month_key,
    beneficiary_id,
    beneficiary_name,
    negative_amount_record_count,
    total_record_count,
    total_amount_received_brl
from mart_recebimentos_by_beneficiary_month
where negative_amount_record_count > 0
order by negative_amount_record_count desc, total_amount_received_brl asc;
```

Total received amount by month:

```sql
select
    launch_month_key,
    launch_month_start_date,
    sum(total_amount_received_brl) as month_total_amount_received_brl,
    sum(total_record_count) as month_total_record_count
from mart_recebimentos_by_beneficiary_month
group by 1, 2
order by 1;
```

Beneficiary-name collisions under the same beneficiary ID:

```sql
select
    launch_month_key,
    beneficiary_id,
    beneficiary_identifier_kind,
    count(distinct beneficiary_name) as distinct_name_count
from mart_recebimentos_by_beneficiary_month
group by 1, 2, 3
having count(distinct beneficiary_name) > 1
order by distinct_name_count desc, beneficiary_id;
```

Largest management-unit totals from the intermediate layer:

```sql
select
    launch_month_key,
    management_unit_id,
    management_unit_name,
    sum(total_amount_received_brl) as total_amount_received_brl,
    sum(record_count) as total_record_count
from int_recebimentos_recursos_por_favorecido_monthly
group by 1, 2, 3
order by total_amount_received_brl desc
limit 20;
```

## Getting Started

This foundation does not require real data to install.

Supported Python versions: 3.11 to 3.13 (`.python-version` pins 3.12). Python
3.14 is excluded because the dbt CLI (`dbt-duckdb`) failed to import on it.

Recommended setup with [uv](https://docs.astral.sh/uv/), which installs the
exact dependency versions recorded in `uv.lock`:

```bash
uv sync --extra dev --extra dashboard
source .venv/bin/activate
```

The `dashboard` extra (Streamlit) is only needed for `gov-spending dashboard`.

Without uv, a plain virtual environment also works, but resolves dependency
versions at install time instead of using the lock file:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

On Windows PowerShell, activate with `.\.venv\Scripts\Activate.ps1` instead.

Check the CLI:

```bash
gov-spending --help
```

Create a local DuckDB database placeholder:

```bash
gov-spending bootstrap-duckdb
```

## Source Profiling Workflow

This repository does not download Portal da Transparencia files automatically. Download one official spending CSV manually and keep the original file name.

Recommended folder pattern:

```bash
mkdir -p data/raw/portal_transparencia/despesas/2025/01
```

Place one downloaded file under that folder, for example:

```text
data/raw/portal_transparencia/despesas/2025/01/20250101_Despesas_Pagamento.csv
```

List discovered raw CSV files:

```bash
gov-spending list-raw-files
```

Profile one selected file:

```bash
gov-spending profile-raw-file --file data/raw/portal_transparencia/despesas/2025/01/20250101_Despesas_Pagamento.csv
```

Or select by a filename substring when exactly one file matches:

```bash
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

## Experimental: API Despesas Documentos Ingestion Workflow

> **Experimental.** The `Despesas` paths (this API ingestion and the CSV
> staging below) are not part of the implemented case study. They have not
> been run end to end against a real downloaded file in this repository, no
> enabled dbt model reads their output, and they are not covered by the CI
> pipeline test. Treat their behavior as provisional.

The project supports a narrow official API extraction path for raw Portal da
Transparencia `despesas/documentos` responses. This is local-first raw
ingestion: responses are stored as JSON exactly as returned by the API, and no
staging, normalization, dbt modeling, or mart logic is applied in this step.

The official API documentation confirms the despesas documents endpoint:

```text
GET https://api.portaldatransparencia.gov.br/api-de-dados/despesas/documentos
```

Confirmed documented query parameters for this endpoint:

- `dataEmissao`: issue date in `DD/MM/YYYY`
- `fase`: spending phase code, where `1` is empenho, `2` is liquidacao, and `3` is pagamento
- `pagina`: page number
- optional filters: `unidadeGestora` and `gestao`

Observed endpoint constraints:

- `dataEmissao`, `fase`, and `pagina` are required
- the request period is limited to one day
- at least one additional filter is required, such as `unidadeGestora` or `gestao`

Configure an API key by registering at the Portal da Transparencia API page and
setting the environment variable named in `config/project.toml`:

```bash
export PORTAL_TRANSPARENCIA_API_KEY="<your-api-key>"
```

Run a narrow extraction:

```bash
gov-spending ingest-despesas-documentos \
  --data-emissao 02/01/2025 \
  --fase 3 \
  --unidade-gestora <VALUE>
```

Default raw output pattern:

```text
data/raw/portal_transparencia_api/despesas_documentos/data_emissao=2025-01-02/fase=3/unidade_gestora=<VALUE>/
```

The extraction writes one raw JSON file per non-empty API page, such as:

```text
page=0001.json
```

It also writes `manifest.json` with request parameters, extraction timestamps,
page count, record count, source endpoint, and the persisted raw files.

What remains provisional:

- raw API response fields are not treated as stable canonical fields yet
- API raw JSON has not replaced the bulk CSV profiling/staging path
- large extractions should still prefer official bulk open-data files, as the Portal documentation recommends
- the first API path covers only `despesas/documentos`, not every Portal da Transparencia endpoint
- valid `unidadeGestora` and `gestao` values must come from official/source context before live extraction

## Experimental: Despesas CSV Staging Workflow

> **Experimental.** The `Despesas` paths (this API ingestion and the CSV
> staging below) are not part of the implemented case study. They have not
> been run end to end against a real downloaded file in this repository, no
> enabled dbt model reads their output, and they are not covered by the CI
> pipeline test. Treat their behavior as provisional.

> Known risk: this path rejects negative `amount_brl` values. The implemented
> recebimentos source contains real negative amounts (`1935627` rows), so a real
> `Despesas` file with reversals may fail staging. Revisit that rule with a real
> profile before relying on this path.

The first staging command is limited to one Portal da Transparencia `Despesas` CSV and its matching profile artifact. It validates that the raw CSV header still matches the profiled columns before writing Parquet.

Run profiling first:

```bash
gov-spending profile-raw-file --file data/raw/portal_transparencia/despesas/2025/01/20250101_Despesas_Pagamento.csv
```

Then stage that same file:

```bash
gov-spending stage-despesas-file --file data/raw/portal_transparencia/despesas/2025/01/20250101_Despesas_Pagamento.csv
```

If the profile artifact is stored somewhere else, pass it explicitly:

```bash
gov-spending stage-despesas-file \
  --file data/raw/portal_transparencia/despesas/2025/01/20250101_Despesas_Pagamento.csv \
  --profile profiling/20250101_Despesas_Pagamento_profile.json
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
- `amount_brl` values are non-negative (provisional; see the known risk above)
- source traceability fields exist and are populated
- `source_row_number` remains positive

The spending stage is inferred from the official `Despesas` file name family, such as `Despesas_Empenho`, `Despesas_Liquidacao`, or `Despesas_Pagamento`.

## Recebimentos Por Favorecido Staging Workflow

The first confirmed non-`Despesas` raw-to-staging path uses the official file:

```text
data/raw/202601_RecebimentosRecursosPorFavorecido.csv
```

Confirmed profile facts:

- encoding: `latin-1`
- delimiter: semicolon
- row count: `300350` (registered version; see `config/source_manifest.toml`)
- column count: `12`
- no null-heavy columns detected in the profiled sample

Download it from the official page, which redirects to a ZIP containing only
this CSV:

```text
https://portaldatransparencia.gov.br/download-de-dados/despesas-favorecidos/202601
```

The exact file version is registered in `config/source_manifest.toml` by
SHA-256, together with its size, row count, archive URL, and retrieval date.
Staging hashes the raw file and refuses it unless the name and hash match a
registered entry, and the expected row count comes from that entry. The Portal
does republish past months (the January 2026 archive was republished on
2026-10-10), so a new download may need to be registered before it can be
staged.

Run staging:

```bash
gov-spending stage-recebimentos-favorecido-file
```

Default output:

```text
data/staging/portal_transparencia/recebimentos_recursos_por_favorecido/202601_RecebimentosRecursosPorFavorecido.parquet
```

Current staging grain:

- one staged row per raw CSV row
- no aggregation, deduplication, or beneficiary classification
- source file name, source path, profile name, and source row number retained
- `Código Favorecido` is staged as text, not an integer
- `Sigla UF` is staged as a location code and may include values such as `EX`
- `Ano e mês do lançamento` is staged as a monthly `YYYY-MM` key
- `Valor Recebido` is parsed as `amount_received_brl`

The full staged file currently contains negative `amount_received_brl` values, so
this source does not enforce a non-negative amount check at staging time.

## Recebimentos dbt Staging Model

The staged Parquet is registered in dbt as an external DuckDB source:

```text
source('portal_transparencia_staging_files', 'recebimentos_recursos_por_favorecido')
```

The first real dbt model for this source is:

```text
stg_portal_transparencia__recebimentos_recursos_por_favorecido
```

This model remains source-aligned and traceable. It keeps `launch_month` as the
staged monthly value and adds conservative analytical helpers:

- `launch_month_key`: integer `YYYYMM`
- `launch_month_start_date`: first day of the month, for date joins
- `launch_year`
- `launch_month_number`

`amount_received_brl` remains signed because negative values were observed in
the real staged output. `beneficiary_id` remains text and geography remains
flexible; `EX` / exterior values are not filtered or remapped.

The first intermediate model above staging is:

```text
int_recebimentos_recursos_por_favorecido_monthly
```

Its grain is one row per month, beneficiary, government body, management unit,
beneficiary location code, and beneficiary municipality name. It produces:

- `total_amount_received_brl`
- `record_count`
- `negative_amount_record_count`

The model keeps signed amounts and does not classify beneficiary type. It also
keeps source row-number bounds and source file name bounds for traceability.
`source_row_number` should be understood as unique within `source_file_name`,
not as a future global key across every source file.

The first mart-ready model above that intermediate layer is:

```text
mart_recebimentos_by_beneficiary_month
```

Its grain is one row per month, `beneficiary_id`, and `beneficiary_name`.
Organizational and geographic fields are intentionally not part of the mart
grain in this first version; they remain available in the intermediate layer.

`beneficiary_name` remains in the mart grain because the real data shows that
`beneficiary_id` alone is not stable enough for beneficiary-level aggregation.
In the current source, multiple names can appear under the same
`beneficiary_id`, so collapsing to month + ID would merge distinct beneficiary
labels too aggressively.

The mart includes:

- `total_amount_received_brl`
- `total_record_count`
- `negative_amount_record_count`

The mart keeps signed amounts and exposes source file and source row-number
bounds for traceability back to the intermediate and staging layers.

## Running dbt

dbt runs from the `dbt/` folder against the DuckDB file under `data/curated/`.
Stage the raw file first, then:

```bash
cp dbt/profiles.yml.example dbt/profiles.yml
cd dbt
dbt build --profiles-dir .
```

`dbt build` loads the seeds, builds the staging views and the intermediate and
mart tables, and runs every data test.

The Recebimentos intermediate model and both marts are incremental by launch
month (`delete+insert` on `launch_month_key`), so a build rebuilds whole months
and never mixes two versions of one month:

- the intermediate model rebuilds a month when the SHA-256 of its staged file
  changed, and every month when the classification seeds changed (it keeps an
  MD5 fingerprint of `treasury_bank_codes` and `treasury_management_units`)
- the marts rebuild the months the intermediate model rebuilt after them
  (`built_at`)
- a change to the model SQL itself, or a restage after a staging code change,
  needs `dbt build --full-refresh`; `gov-spending restage-recebimentos
  --dbt-build` passes it

Over the full history (over 100 million rows) DuckDB needs a memory cap. The
example profile sets `memory_limit: "6GB"`, and DuckDB spills to disk past it.
A `profiles.yml` copied before that change has no cap: add the `settings`
block from the example, or systemd-oomd may kill the whole terminal mid-build. The project var
`recebimentos_staged_parquet` controls the input. It defaults to a glob that
reads every staged month under
`data/staging/portal_transparencia/recebimentos_recursos_por_favorecido/`;
override it with `--vars` to build against another file or glob.

Each staged row carries `source_registered_row_count`, the row count registered
for its file version in the source manifest. The
`assert_recebimentos_source_row_count` test checks every staged file against
it, so no month can lose or gain rows between the manifest and the mart. A
warn-level test also flags any file whose launch months differ from the month
in its file name.

## Keeping The Data Current

The Portal publishes one archive per month (`YYYYMM_Despesas.zip`, holding a
single `YYYYMM_RecebimentosRecursosPorFavorecido.csv`) from 2013-12 onward. It
updates the current month every day and republishes past months without notice
(on 2026-10-22 the server reported that day's date for 2026-02 through 2026-10
and also for older months such as 2016-12 and 2020-04). There is no delay after which a month is
final, so the sync job detects changes instead of waiting a fixed period:

```bash
gov-spending sync-recebimentos --dry-run          # report what changed, download nothing
gov-spending sync-recebimentos --dbt-build        # current month + the 12 before it
gov-spending sync-recebimentos --all --dbt-build  # every month since 2013-12
gov-spending sync-recebimentos --since 2024-01 --until 2024-12
```

For each month it:

1. asks the server for `Last-Modified` and size (HEAD) and skips the month when
   both match the last run and the month is already staged
2. downloads the archive into a new folder,
   `data/raw/portal_transparencia/recebimentos_recursos_por_favorecido/YYYYMM/<UTC timestamp>/`,
   and refuses it unless it holds exactly the expected CSV within a size limit
3. hashes the CSV; when the content is unchanged it keeps the staged month and
   deletes the new folder
4. otherwise profiles the CSV, stages it against a one-entry manifest next to the
   download, and only then appends the version to
   `data/sync/recebimentos_source_manifest.toml`
5. replaces `data/staging/.../YYYYMM_RecebimentosRecursosPorFavorecido.parquet`
   atomically and deletes the extracted CSV (`--keep-csv` keeps it); the archive
   and profile stay as the record of that version

Each month prints one status: `missing`, `unchanged`, `same_content`, `staged`,
`would_download`, `failed` (retried next run), or `blocked`. A month registered
while it was still the current month is flagged `is_open_month = true`.

The reviewed `config/source_manifest.toml` stays the pinned reference for the
documented January 2026 results; the sync manifest under `data/` is machine
written and not committed. Republished versions are kept side by side, so a
changed month can always be compared with the version it replaced.

**Rate limits.** The download server sits behind an AWS WAF. The first
full-history run (about 170 HEAD requests in a few minutes) was answered with
HTTP 405 and `x-amzn-waf-action: captcha` for every request. Sync therefore
waits `--request-interval` seconds between months (3 by default), checks only
the recent window by default, treats a WAF challenge or HTTP 429 as `blocked`,
and stops the run instead of retrying. It does not try to get around the
challenge; wait a few hours and run again.

**Restaging without downloading.** After a staging change, rebuild every synced
month from the archives already on disk; the CSV must still match its registered
SHA-256:

```bash
gov-spending restage-recebimentos --dbt-build
```

**Scheduling.** On Linux, install systemd user timers that run the recent-window
sync daily at 06:00 and a full `--all` sweep on Sundays at 07:00, each followed
by `dbt build`:

```bash
scripts/install_sync_timer.sh
journalctl --user -u gov-spending-sync.service -u gov-spending-sync-full.service
```

The Portal regenerates its archives shortly after 00:00 UTC (about 21:00 in
Brasilia), so morning runs pick up the previous evening's files. An advisory
lock keeps a manual run and a scheduled run from syncing at the same time.

On Windows the sync and the lock work the same way, but there is no installer
for the schedule: point Task Scheduler at
`.venv\Scripts\gov-spending.exe sync-recebimentos --dbt-build`, run from the
repository folder.

A full history download is about 3 GB of archives; staging takes roughly a
minute per month on a desktop machine, so `--all` from scratch runs for a few
hours.

## Dashboard

```bash
gov-spending dashboard            # http://localhost:8501
```

Streamlit runs headless: it prints the URL instead of opening a browser, and
skips its first-run email prompt.

A Streamlit page over the dbt marts, in Brazilian Portuguese for the public
audience:

- period slider over the months loaded, superior government body filter, and
  beneficiary identifier kind filter
- total received, record count, negative records, and months with data
- monthly totals on a time axis: months not loaded locally show as gaps, and a
  month registered while still open is drawn lighter and labelled partial
- rankings by superior government body and by identifier kind
- the largest CNPJ and foreign beneficiaries in the period; masked CPFs are people
  and are never ranked

Charts read `mart_recebimentos_monthly_summary`, a rollup of month x superior
body x identifier kind that stays small over the full history. Queries use
short-lived read-only DuckDB connections, so the scheduled sync can rebuild the
database while the page is open. The light and dark themes follow the viewer.

The dashboard is kept local by decision: the page binds to `localhost` and is
not hosted anywhere. Every identifier kind except the Treasury cash
operations (`treasury_operation`) and the Treasury management units
(`treasury_management_unit`) is selected by default, so the headline total
leaves out public debt service, bond issuance, Treasury and INSS transfers,
and payments to the Treasury's own units (see [Treasury Codes](#treasury-codes));
select those kinds to include them.

The default view, the last 13 months of the synced history (October 2025 to
the open October 2026), light theme:

<img src="docs/assets/screenshots/08-dashboard-light.png" alt="Dashboard default view, last 13 months, light theme" width="900" />

The same view in the dark theme:

<img src="docs/assets/screenshots/09-dashboard-dark.png" alt="Dashboard default view, last 13 months, dark theme" width="900" />

## End-To-End Test On A Synthetic Sample

The real source file is not committed, so the full pipeline is also exercised
on a 20-row synthetic sample:

```text
tests/fixtures/recebimentos_sample/202601_RecebimentosRecursosPorFavorecido.csv
```

It keeps the official format (latin-1, semicolon delimiter, quoted fields,
CRLF line endings, `MM/YYYY` months, Brazilian decimals) and covers the
identifier kinds found in the source except `treasury_payment` and
`intergovernmental_transfer`, negative amounts, an `EX` / exterior beneficiary,
and one masked-CPF name collision. Beneficiary names and identifiers are
fictitious. It is pinned by its own manifest, `tests/fixtures/source_manifest.toml`,
and regenerated with `tests/fixtures/make_recebimentos_sample.py`.

`tests/test_recebimentos_e2e.py` profiles the sample, stages it, runs
`dbt build` into a temporary DuckDB file, and checks the identifier kinds,
mart row count, and collision result:

```bash
pytest tests/test_recebimentos_e2e.py
```

GitHub Actions (`.github/workflows/ci.yml`) runs `ruff check`, the full pytest
suite, and therefore this sample `dbt build`, on Python 3.11, 3.12, and 3.13
with the dependency versions locked in `uv.lock`.

## Limitations And Future Work

Current limitations are explicit:

- the pipeline screenshots and the Excel samples still cover January 2026 only;
  the case study numbers and the dashboard screenshots cover the full history
- `sync-recebimentos --all` stops when the Portal's WAF challenges the machine,
  so a first full sync may need more than one run
- a full `dbt build --full-refresh` takes about three and a half hours and
  spills to disk under the 6 GB DuckDB cap; daily builds are incremental, but
  the data tests still scan the full history, so a build with no changed month
  still takes about 15 minutes
- the `RB` classification is read from the published code names and reviewed
  by hand; new codes need the same review when the dbt test warns
- the Treasury management units are a reviewed list of `8` STN units; leaving
  them and the cash operations out does not remove every transfer inside the
  government, because the other federal management units (`management_unit`,
  R$ 1.50 trillion net) stay in the default totals
- the dbt CLI does not run on Python 3.14 (upstream import error in `dbt-duckdb`), so the project is pinned to Python 3.11-3.13
- `beneficiary_id` is not yet a reliable beneficiary master key
- beneficiary type classification is intentionally deferred
- organizational and geographic attributes are not part of the first mart grain
- the repository does not yet provide a broad multi-source unified spending mart

Reasonable next steps, without changing the project character:

- review which `management_unit` flows are transfers inside the government
  (for example, by matching the paying and the receiving unit)
- over the full history, `4449` CNPJ, foreign, and management-unit IDs change name
  across months and none within a month; a name history per CNPJ could
  support a reference layer
- decide whether a separate beneficiary reference layer is justified by repeated evidence
- extend the same conservative pattern to additional official source families

## Official Source Starting Point

The initial source strategy is based on the Portal da Transparencia open data download area, especially the public spending files listed under "Despesas publicas", including "Documentos de empenho, liquidacao e pagamento" and "Execucao da despesa". See [docs/source_catalog.md](docs/source_catalog.md) for details and source links checked during repository creation.

Current profiling status in git-tracked documentation: no real raw CSV file or profile artifact is committed because raw data and local profiles are ignored. The staging implementation reads the ignored local profile artifact and uses only columns present in that artifact's `columns` list. Run `gov-spending profile-raw-file` after placing a manually downloaded file under `data/raw/`, then run `gov-spending stage-despesas-file` against the same file.

## Analytical Output Samples

Supplementary analytical output examples for the current implemented case study
are included as supporting material.

- Excel workbook: [docs/assets/outputs/public_spending_case_outputs.xlsx](docs/assets/outputs/public_spending_case_outputs.xlsx)
- Supporting Excel screenshots: [docs/assets/screenshots/excel/](docs/assets/screenshots/excel/)

## License

The code and documentation in this repository are released under the
[MIT License](LICENSE), Copyright (c) 2026 PhaifferTech.

The source data is not part of this repository. It is published by the
Controladoria-Geral da Uniao through the Portal da Transparencia and remains
subject to that portal's own terms of use.
