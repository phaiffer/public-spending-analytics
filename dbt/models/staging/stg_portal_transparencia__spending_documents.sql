{{ config(enabled=false) }}

-- Disabled scaffold.
-- Enable after ingestion writes a stable Parquet dataset and source columns are profiled.

select
    cast(null as varchar) as source_file_name,
    cast(null as varchar) as spending_document_id,
    cast(null as date) as spending_date,
    cast(null as integer) as fiscal_year,
    cast(null as varchar) as government_body_id,
    cast(null as varchar) as government_body_name,
    cast(null as varchar) as beneficiary_id,
    cast(null as varchar) as beneficiary_name,
    cast(null as varchar) as spending_stage,
    cast(null as decimal(18, 2)) as amount_brl
where false
