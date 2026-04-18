with staged as (

    select
        count(*) as record_count,
        sum(case when amount_received_brl < 0 then 1 else 0 end) as negative_amount_record_count,
        sum(amount_received_brl) as total_amount_received_brl
    from {{ ref('stg_portal_transparencia__recebimentos_recursos_por_favorecido') }}

),

monthly as (

    select
        sum(record_count) as record_count,
        sum(negative_amount_record_count) as negative_amount_record_count,
        sum(total_amount_received_brl) as total_amount_received_brl
    from {{ ref('int_recebimentos_recursos_por_favorecido_monthly') }}

)

select *
from staged
cross join monthly
where staged.record_count != monthly.record_count
   or staged.negative_amount_record_count != monthly.negative_amount_record_count
   or staged.total_amount_received_brl != monthly.total_amount_received_brl
