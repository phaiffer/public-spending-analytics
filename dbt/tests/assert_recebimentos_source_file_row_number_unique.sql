select
    source_file_name,
    source_row_number
from {{ ref('stg_portal_transparencia__recebimentos_recursos_por_favorecido') }}
group by 1, 2
having count(*) > 1
