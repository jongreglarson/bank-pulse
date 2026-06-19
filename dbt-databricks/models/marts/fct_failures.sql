select
    cert,
    institution_name,
    failure_date,
    {{ fiscal_quarter('failure_date') }}                                            as failure_quarter,
    cast(extract(year from failure_date) as int)                                    as failure_year,
    savings_rate_class,
    resolution_type,
    estimated_loss_millions,
    deposits_at_failure_thousands,
    assets_at_failure_thousands
from {{ ref('stg_failures') }}
where failure_date is not null
