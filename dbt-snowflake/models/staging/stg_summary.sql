with source as (
    select * from {{ source('bronze', 'summary') }}
)

select
    try_to_date(raw_data:repdte::varchar, 'YYYY-MM-DD')         as report_date,
    raw_data:asset::bigint                                      as total_assets_thousands,
    raw_data:dep::bigint                                        as total_deposits_thousands,
    raw_data:intinc::bigint                                     as interest_income_thousands,
    raw_data:nonii::bigint                                      as noninterest_income_thousands,
    raw_data:netinc::bigint                                     as net_income_thousands,
    raw_data:lnlsnet::bigint                                    as net_loans_thousands,
    loaded_at
from source
where raw_data:repdte is not null
