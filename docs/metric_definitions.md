# Metric Definitions

This document defines the intended MVP metrics for Brazilian federal public spending analytics.

The definitions below are analytical targets. They must be validated against real source columns and source dictionaries before being treated as final.

## Profiling Status

Real raw spending CSV files and profile artifacts are local-only and ignored by git, so this document does not publish file-specific observed columns. The profiling workflow captures observed columns, row count, sample records, inferred basic types, and null-heavy columns for one manually downloaded file.

The first staging implementation can now convert one profiled Portal da Transparencia `Despesas` CSV to Parquet. This does not finalize the mart grain or the metric definitions.

Metrics should move from provisional to confirmed only after:

- the relevant source amount columns are observed
- document or item grain is understood
- spending-stage context is confirmed
- date fields are mapped safely
- beneficiary and government-body identifiers are evaluated for nulls and uniqueness
- the staged Parquet output is reviewed against the profile artifact

## Core Concepts

### Commitment

Commitment represents spending that the government has formally reserved for a specific purpose or obligation.

Portuguese source concept: `empenho`.

Candidate metric:

```text
committed_amount_brl = sum(amount_brl where spending_stage = 'commitment')
```

Current status: provisional until an `Empenho` file or equivalent commitment source is profiled.

### Liquidation

Liquidation represents spending recognized as delivered, fulfilled, or payable according to public accounting rules.

Portuguese source concept: `liquidacao`.

Candidate metric:

```text
liquidated_amount_brl = sum(amount_brl where spending_stage = 'liquidation')
```

Current status: provisional until a `Liquidacao` file or equivalent liquidation source is profiled.

### Payment

Payment represents spending effectively paid.

Portuguese source concept: `pagamento`.

Candidate metric:

```text
paid_amount_brl = sum(amount_brl where spending_stage = 'payment')
```

Current status: provisional until a `Pagamento` file or equivalent payment source is profiled.

## MVP KPIs

### Committed Amount

Total committed value in Brazilian reais.

Formula:

```text
sum(committed_amount_brl)
```

Expected dimensions:

- date
- government body
- beneficiary, where available
- fiscal year

### Liquidated Amount

Total liquidated value in Brazilian reais.

Formula:

```text
sum(liquidated_amount_brl)
```

Expected dimensions:

- date
- government body
- beneficiary, where available
- fiscal year

### Paid Amount

Total paid value in Brazilian reais.

Formula:

```text
sum(paid_amount_brl)
```

Expected dimensions:

- date
- government body
- beneficiary
- fiscal year

### Liquidation Rate

Share of committed amount that has been liquidated.

Formula:

```text
liquidation_rate = liquidated_amount_brl / committed_amount_brl
```

Rules:

- Return null when committed amount is zero or unavailable.
- Do not compare across incompatible grains.

### Payment Rate

Share of committed amount that has been paid.

Formula:

```text
payment_rate = paid_amount_brl / committed_amount_brl
```

Rules:

- Return null when committed amount is zero or unavailable.
- Interpret carefully when commitments and payments are not linked one-to-one.

### Open Commitment Amount

Committed value not yet paid.

Formula:

```text
open_commitment_amount_brl = committed_amount_brl - paid_amount_brl
```

Rules:

- Only calculate when commitment and payment values are aligned by a defensible key or aggregation grain.
- Avoid presenting this as unpaid debt without accounting validation.

### Beneficiary Concentration

Share of paid amount represented by the largest beneficiaries.

Example formula:

```text
top_n_beneficiary_share = paid_amount_brl for top N beneficiaries / total_paid_amount_brl
```

Rules:

- Beneficiary identifiers must be normalized.
- Missing, masked, or list-based beneficiaries should be documented separately.

## Standard Dimensions

### Date

Planned fields, subject to source profiling:

- `date_key`
- `date`
- `year`
- `month`
- `quarter`
- `fiscal_year`

### Government Body

Planned fields, subject to source profiling:

- `government_body_id`
- `government_body_name`
- `superior_body_id`, if available
- `superior_body_name`, if available

### Beneficiary

Planned fields, subject to source profiling:

- `beneficiary_id`
- `beneficiary_name`
- `beneficiary_type`, if derivable

Important caveat:

Some payment structures may involve final beneficiaries, creditor lists, or supporting list files. The MVP should avoid flattening these relationships until the source files are profiled.

### Spending Stage

Controlled values:

- `commitment`
- `liquidation`
- `payment`

Source labels should be mapped into these English analytical values.

## Data Quality Expectations

Initial quality checks should include:

- required staged canonical fields are not null
- spending stage values are accepted
- amount fields parse as decimals
- amount values are non-negative where the selected spending source is expected to carry positive spending values
- source file metadata and source row numbers are retained
- duplicate handling rules are explicit

The first profiling artifact should be used to decide which checks are realistic for the selected source file. For example, a null-heavy beneficiary field may be expected in some file types and problematic in others.

For the first `Despesas` staging implementation, `amount_brl` is the only required canonical field. `spending_document_id` remains optional and provisional until the real observed source columns support one stable document identifier across the selected file type.

## Limitations

- These definitions are not a substitute for official accounting interpretation.
- Source dictionaries must be reviewed before final implementation.
- Some metrics may require document linkage tables.
- Payments to final beneficiaries may require separate modeling from direct payment documents.
