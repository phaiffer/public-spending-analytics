-- Every staged source file must keep exactly the row count registered for its
-- version in the source manifest (carried on each row by the Python staging job).

select
    source_file_name,
    count(*) as staged_row_count,
    min(source_registered_row_count) as registered_row_count
from {{ ref('stg_portal_transparencia__recebimentos_recursos_por_favorecido') }}
group by 1
having count(*) != min(source_registered_row_count)
    or min(source_registered_row_count) != max(source_registered_row_count)
