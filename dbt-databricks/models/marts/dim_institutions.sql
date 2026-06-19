with ranked as (
    select
        cert,
        institution_name,
        city,
        state_name,
        is_active,
        report_date                                                                 as as_of_date,
        row_number() over (partition by cert order by report_date desc)             as rn
    from {{ ref('stg_institutions') }}
)

select
    cert,
    institution_name,
    city,
    state_name,
    is_active,
    as_of_date
from ranked
where rn = 1
