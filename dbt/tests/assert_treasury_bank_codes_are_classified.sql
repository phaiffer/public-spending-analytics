{{ config(severity='warn') }}

-- Every RB code in the source should be reviewed in the treasury_bank_codes seed.
-- An unreviewed code stays in the data as treasury_unclassified; this warning lists
-- it with its published name so it can be added to the seed.

select
    beneficiary_id,
    beneficiary_name,
    min(launch_month_key) as first_month_key,
    count(*) as month_name_rows
from {{ ref('mart_recebimentos_by_beneficiary_month') }}
where beneficiary_identifier_kind = 'treasury_unclassified'
group by 1, 2
