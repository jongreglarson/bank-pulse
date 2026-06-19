with source as (
    select * from {{ source('bronze', 'institutions') }}
)

select
    cast(cert as int)                                           as cert,
    trim(name)                                                  as institution_name,
    trim(city)                                                  as city,
    trim(stname)                                                as state_name,
    cast(asset as bigint)                                       as total_assets_thousands,
    cast(dep as bigint)                                         as total_deposits_thousands,
    cast(netinc as bigint)                                      as net_income_thousands,
    to_date(cast(repdte as string), 'yyyy-MM-dd')               as report_date,
    cast(active as boolean)                                     as is_active,
    _loaded_at
from source
where cert is not null
