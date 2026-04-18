select *
from {{ ref('stg_portal_transparencia__recebimentos_recursos_por_favorecido') }}
where launch_month_key != cast(replace(launch_month, '-', '') as integer)
   or launch_month_start_date != cast(strptime(launch_month || '-01', '%Y-%m-%d') as date)
   or launch_year != cast(substr(launch_month, 1, 4) as integer)
   or launch_month_number != cast(substr(launch_month, 6, 2) as integer)
