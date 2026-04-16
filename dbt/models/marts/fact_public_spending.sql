{{ config(enabled=false) }}

-- Disabled scaffold.
-- Enable after the source grain and document linkage rules are validated.

select
    spending_event_id,
    source_file_name,
    spending_document_id,
    spending_date,
    fiscal_year,
    government_body_id,
    beneficiary_id,
    spending_stage,
    amount_brl
from {{ ref('int_spending_stage_events') }}
