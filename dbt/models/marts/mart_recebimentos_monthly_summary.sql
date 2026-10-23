{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='launch_month_key',
        on_schema_change='fail'
    )
}}

-- Small monthly rollup for dashboards: one row per month, superior government
-- body, and beneficiary identifier kind. It stays a few thousand rows per year of
-- history, so charts never scan the beneficiary-level tables.

select
    launch_month_key,
    launch_month_start_date,
    launch_year,
    launch_month_number,
    superior_government_body_id,
    superior_government_body_name,
    beneficiary_identifier_kind,
    sum(total_amount_received_brl) as total_amount_received_brl,
    sum(record_count) as total_record_count,
    sum(negative_amount_record_count) as negative_amount_record_count,
    max(built_at) as built_at
from {{ ref('int_recebimentos_recursos_por_favorecido_monthly') }}
{% if is_incremental() %}
-- Only the months the intermediate model rebuilt after this mart.
where launch_month_key in (
    {{ recebimentos_rebuilt_intermediate_months(ref('int_recebimentos_recursos_por_favorecido_monthly')) }}
)
{% endif %}
group by
    launch_month_key,
    launch_month_start_date,
    launch_year,
    launch_month_number,
    superior_government_body_id,
    superior_government_body_name,
    beneficiary_identifier_kind
