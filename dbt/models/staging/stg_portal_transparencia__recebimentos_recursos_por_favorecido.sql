with source as (

    select *
    from {{ source('portal_transparencia_staging_files', 'recebimentos_recursos_por_favorecido') }}

),

treasury_bank_codes as (

    select beneficiary_id, treasury_flow_class
    from {{ ref('treasury_bank_codes') }}

),

typed as (

    select
        cast(source_system as varchar) as source_system,
        cast(source_family as varchar) as source_family,
        cast(source_file_name as varchar) as source_file_name,
        cast(source_file_path as varchar) as source_file_path,
        cast(source_file_sha256 as varchar) as source_file_sha256,
        cast(source_registered_row_count as bigint) as source_registered_row_count,
        cast(source_profile_name as varchar) as source_profile_name,
        cast(source_row_number as bigint) as source_row_number,

        cast(source.beneficiary_id as varchar) as beneficiary_id,
        -- Shape of beneficiary_id observed in the source. CPFs are published masked
        -- (only 6 middle digits visible), so one masked value can cover several people.
        -- RB codes are Treasury bank operations, classified one by one in the
        -- treasury_bank_codes seed; TN codes are transfers to states and municipalities.
        case
            when regexp_full_match(source.beneficiary_id, '[0-9]{14}') then 'cnpj'
            when regexp_full_match(source.beneficiary_id, '\*\*\*\.[0-9]{3}\.[0-9]{3}-\*\*')
                then 'cpf_masked'
            when regexp_full_match(source.beneficiary_id, 'EX[A-Z0-9]{7}') then 'foreign_code'
            when regexp_full_match(source.beneficiary_id, '-[0-9]+') then 'placeholder'
            when regexp_full_match(source.beneficiary_id, 'RB[0-9]{7}')
                then coalesce(treasury_bank_codes.treasury_flow_class, 'treasury_unclassified')
            when regexp_full_match(source.beneficiary_id, 'TN[0-9]{7}')
                then 'intergovernmental_transfer'
            else 'other_code'
        end as beneficiary_identifier_kind,
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
    left join treasury_bank_codes
        on source.beneficiary_id = treasury_bank_codes.beneficiary_id

)

select *
from typed
