select
    launch_month_key,
    launch_month_start_date,
    launch_year,
    launch_month_number,
    beneficiary_id,
    beneficiary_name,
    sum(total_amount_received_brl) as total_amount_received_brl,
    sum(record_count) as total_record_count,
    sum(negative_amount_record_count) as negative_amount_record_count,
    min(first_source_file_name) as first_source_file_name,
    max(last_source_file_name) as last_source_file_name,
    min(first_source_row_number) as first_source_row_number,
    max(last_source_row_number) as last_source_row_number
from {{ ref('int_recebimentos_recursos_por_favorecido_monthly') }}
group by
    launch_month_key,
    launch_month_start_date,
    launch_year,
    launch_month_number,
    beneficiary_id,
    beneficiary_name
