select
    spending_stage,
    source_label_pt,
    stage_order,
    description
from {{ ref('spending_stage_seed') }}
