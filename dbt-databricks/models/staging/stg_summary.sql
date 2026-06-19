with source as (
    select * from {{ source('bronze', 'summary') }}
)

select
    to_date(cast(repdte as string), 'yyyy-MM-dd')               as report_date,
    cast(asset as bigint)                                       as total_assets_thousands,
    cast(dep as bigint)                                         as total_deposits_thousands,
    cast(intinc as bigint)                                      as interest_income_thousands,
    cast(nonii as bigint)                                       as noninterest_income_thousands,
    cast(netinc as bigint)                                      as net_income_thousands,
    cast(lnlsnet as bigint)                                     as net_loans_thousands,
    _loaded_at
from source
where repdte is not null
