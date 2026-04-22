with intermediate as (

    select
        sum(total_amount_received_brl) as total_amount_received_brl,
        sum(record_count) as total_record_count,
        sum(negative_amount_record_count) as negative_amount_record_count
    from {{ ref('int_recebimentos_recursos_por_favorecido_monthly') }}

),

mart as (

    select
        sum(total_amount_received_brl) as total_amount_received_brl,
        sum(total_record_count) as total_record_count,
        sum(negative_amount_record_count) as negative_amount_record_count
    from {{ ref('mart_recebimentos_by_beneficiary_month') }}

)

select *
from intermediate
cross join mart
where intermediate.total_amount_received_brl != mart.total_amount_received_brl
   or intermediate.total_record_count != mart.total_record_count
   or intermediate.negative_amount_record_count != mart.negative_amount_record_count
