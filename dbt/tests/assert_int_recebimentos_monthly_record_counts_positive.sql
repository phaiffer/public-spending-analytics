select *
from {{ ref('int_recebimentos_recursos_por_favorecido_monthly') }}
where record_count < 1
   or negative_amount_record_count < 0
   or negative_amount_record_count > record_count
