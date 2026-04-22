select *
from {{ ref('mart_recebimentos_by_beneficiary_month') }}
where total_record_count < 1
   or negative_amount_record_count < 0
   or negative_amount_record_count > total_record_count
