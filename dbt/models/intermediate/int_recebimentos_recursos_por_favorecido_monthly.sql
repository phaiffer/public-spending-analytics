with staged as (

    select *
    from {{ ref('stg_portal_transparencia__recebimentos_recursos_por_favorecido') }}

),

monthly as (

    select
        launch_month_key,
        launch_month_start_date,
        launch_year,
        launch_month_number,

        beneficiary_id,
        beneficiary_name,
        beneficiary_location_code,
        beneficiary_municipality_name,
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
        max(source_row_number) as last_source_row_number
    from staged
    group by
        launch_month_key,
        launch_month_start_date,
        launch_year,
        launch_month_number,
        beneficiary_id,
        beneficiary_name,
        beneficiary_location_code,
        beneficiary_municipality_name,
        government_body_id,
        government_body_name,
        management_unit_id,
        management_unit_name

)

select *
from monthly
