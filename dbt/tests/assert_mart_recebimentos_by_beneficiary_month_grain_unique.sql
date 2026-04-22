select
    launch_month_key,
    beneficiary_id,
    beneficiary_name
from {{ ref('mart_recebimentos_by_beneficiary_month') }}
group by 1, 2, 3
having count(*) > 1
