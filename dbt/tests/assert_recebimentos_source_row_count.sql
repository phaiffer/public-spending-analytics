select *
from (
    select count(*) as row_count
    from {{ ref('stg_portal_transparencia__recebimentos_recursos_por_favorecido') }}
)
where row_count != 300391
