{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='launch_month_key',
        on_schema_change='fail'
    )
}}

with staged as (

    select *
    from {{ ref('stg_portal_transparencia__recebimentos_recursos_por_favorecido') }}
    {% if is_incremental() %}
    -- Only the months whose staged file or classification changed since the last build.
    where launch_month_key in (
        {{ recebimentos_changed_staged_months(ref('stg_portal_transparencia__recebimentos_recursos_por_favorecido')) }}
    )
    {% endif %}

),

monthly as (

    select
        launch_month_key,
        launch_month_start_date,
        launch_year,
        launch_month_number,

        beneficiary_id,
        beneficiary_identifier_kind,
        beneficiary_name,
        beneficiary_location_code,
        beneficiary_municipality_name,
        superior_government_body_id,
        superior_government_body_name,
        government_body_id,
        government_body_name,
        management_unit_id,
        management_unit_name,

        count(*) as record_count,
        sum(case when amount_received_brl < 0 then 1 else 0 end) as negative_amount_record_count,
        sum(amount_received_brl) as total_amount_received_brl,
        min(source_file_name) as first_source_file_name,
        max(source_file_name) as last_source_file_name,
        min(source_row_number) as first_source_row_number,
        max(source_row_number) as last_source_row_number,
        max(source_file_sha256) as source_file_sha256
    from staged
    group by
        launch_month_key,
        launch_month_start_date,
        launch_year,
        launch_month_number,
        beneficiary_id,
        beneficiary_identifier_kind,
        beneficiary_name,
        beneficiary_location_code,
        beneficiary_municipality_name,
        superior_government_body_id,
        superior_government_body_name,
        government_body_id,
        government_body_name,
        management_unit_id,
        management_unit_name

)

select
    monthly.*,
    {{ recebimentos_classification_fingerprint() }} as classification_fingerprint,
    current_timestamp as built_at
from monthly
