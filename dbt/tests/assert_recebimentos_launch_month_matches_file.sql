{{ config(severity='warn') }}

-- Each monthly file is named YYYYMM_RecebimentosRecursosPorFavorecido.csv. Warn if a
-- file carries launches from another month, because monthly totals would then mix
-- the two months.

select
    source_file_name,
    launch_month,
    count(*) as row_count
from {{ ref('stg_portal_transparencia__recebimentos_recursos_por_favorecido') }}
where cast(launch_month_key as varchar) != substr(source_file_name, 1, 6)
group by 1, 2
