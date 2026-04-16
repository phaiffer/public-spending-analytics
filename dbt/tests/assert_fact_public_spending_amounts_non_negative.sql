{{ config(enabled=false) }}

-- Enable when fact_public_spending is implemented.

select *
from {{ ref('fact_public_spending') }}
where amount_brl < 0
