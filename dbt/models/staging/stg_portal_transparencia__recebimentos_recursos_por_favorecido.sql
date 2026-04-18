with source as (

    select *
    from {{ source('portal_transparencia_staging_files', 'recebimentos_recursos_por_favorecido') }}

),

typed as (

    select
        cast(source_system as varchar) as source_system,
        cast(source_family as varchar) as source_family,
        cast(source_file_name as varchar) as source_file_name,
        cast(source_file_path as varchar) as source_file_path,
        cast(source_profile_name as varchar) as source_profile_name,
        cast(source_row_number as bigint) as source_row_number,

        cast(beneficiary_id as varchar) as beneficiary_id,
        cast(beneficiary_name as varchar) as beneficiary_name,
        cast(beneficiary_location_code as varchar) as beneficiary_location_code,
        cast(beneficiary_municipality_name as varchar) as beneficiary_municipality_name,
        cast(superior_government_body_id as varchar) as superior_government_body_id,
        cast(superior_government_body_name as varchar) as superior_government_body_name,
        cast(government_body_id as varchar) as government_body_id,
        cast(government_body_name as varchar) as government_body_name,
        cast(management_unit_id as varchar) as management_unit_id,
        cast(management_unit_name as varchar) as management_unit_name,

        cast(launch_month as varchar) as launch_month,
        cast(replace(launch_month, '-', '') as integer) as launch_month_key,
        cast(strptime(launch_month || '-01', '%Y-%m-%d') as date) as launch_month_start_date,
        cast(substr(launch_month, 1, 4) as integer) as launch_year,
        cast(substr(launch_month, 6, 2) as integer) as launch_month_number,

        cast(amount_received_brl as decimal(18, 4)) as amount_received_brl
    from source

)

select *
from typed
