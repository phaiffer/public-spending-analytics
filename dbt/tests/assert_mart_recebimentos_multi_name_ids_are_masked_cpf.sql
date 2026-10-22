{{ config(severity='warn') }}

-- Within one month, every beneficiary_id with more than one beneficiary_name is a
-- masked CPF in the registered source versions. Warn if another identifier kind
-- starts showing name collisions, because that would be a different identity
-- problem. Across months, CNPJs do change names (companies are renamed), so the
-- check is per month.

select
    launch_month_key,
    beneficiary_id,
    beneficiary_identifier_kind,
    count(distinct beneficiary_name) as beneficiary_name_count
from {{ ref('mart_recebimentos_by_beneficiary_month') }}
where beneficiary_identifier_kind != 'cpf_masked'
group by 1, 2, 3
having count(distinct beneficiary_name) > 1
