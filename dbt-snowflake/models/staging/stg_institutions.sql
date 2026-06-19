with source as (
    select * from {{ source('bronze', 'institutions') }}
)

select
    raw_data:cert::int                                          as cert,
    trim(raw_data:name::varchar)                                as institution_name,
    trim(raw_data:city::varchar)                                as city,
    trim(raw_data:stname::varchar)                              as state_name,
    raw_data:asset::bigint                                      as total_assets_thousands,
    raw_data:dep::bigint                                        as total_deposits_thousands,
    raw_data:netinc::bigint                                     as net_income_thousands,
    try_to_date(raw_data:repdte::varchar, 'YYYY-MM-DD')         as report_date,
    raw_data:active::boolean                                    as is_active,
    loaded_at
from source
where raw_data:cert is not null
