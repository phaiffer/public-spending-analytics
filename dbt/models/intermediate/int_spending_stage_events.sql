{{ config(enabled=false) }}

-- Disabled scaffold.
-- This model will standardize commitment, liquidation, and payment records.

select
    cast(null as varchar) as spending_event_id,
    source_file_name,
    spending_document_id,
    spending_date,
    fiscal_year,
    government_body_id,
    government_body_name,
    beneficiary_id,
    beneficiary_name,
    spending_stage,
    amount_brl
from {{ ref('stg_portal_transparencia__spending_documents') }}
