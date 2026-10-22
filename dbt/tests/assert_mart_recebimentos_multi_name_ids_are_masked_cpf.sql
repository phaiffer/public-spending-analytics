{{ config(severity='warn') }}

-- In the registered source version every beneficiary_id with more than one
-- beneficiary_name is a masked CPF. Warn if another identifier kind starts
-- showing name collisions, because that would be a different identity problem.

select
    beneficiary_id,
    beneficiary_identifier_kind,
    count(distinct beneficiary_name) as beneficiary_name_count
from {{ ref('mart_recebimentos_by_beneficiary_month') }}
where beneficiary_identifier_kind != 'cpf_masked'
group by 1, 2
having count(distinct beneficiary_name) > 1
