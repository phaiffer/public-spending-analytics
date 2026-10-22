with intermediate as (

    select
        sum(total_amount_received_brl) as total_amount_received_brl,
        sum(record_count) as total_record_count,
        sum(negative_amount_record_count) as negative_amount_record_count
    from {{ ref('int_recebimentos_recursos_por_favorecido_monthly') }}

),

summary as (

    select
        sum(total_amount_received_brl) as total_amount_received_brl,
        sum(total_record_count) as total_record_count,
        sum(negative_amount_record_count) as negative_amount_record_count
    from {{ ref('mart_recebimentos_monthly_summary') }}

)

select *
from intermediate
cross join summary
where intermediate.total_amount_received_brl != summary.total_amount_received_brl
   or intermediate.total_record_count != summary.total_record_count
   or intermediate.negative_amount_record_count != summary.negative_amount_record_count
