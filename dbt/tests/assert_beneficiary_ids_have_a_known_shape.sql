{{ config(severity='warn') }}

-- Every beneficiary_id should match one of the shapes classified in staging. An ID
-- of a new shape stays in the data as other_code; this warning lists it with its
-- published name so the classification can be extended.

select
    beneficiary_id,
    beneficiary_name,
    min(launch_month_key) as first_month_key,
    count(*) as month_name_rows
from {{ ref('mart_recebimentos_by_beneficiary_month') }}
where beneficiary_identifier_kind = 'other_code'
group by 1, 2
