with source as (
    select * from {{ source('bronze', 'history') }}
)

select
    raw_data:cert::int                                          as cert,
    trim(raw_data:instname::varchar)                            as institution_name,
    trim(raw_data:class::varchar)                               as charter_class,
    trim(raw_data:pcity::varchar)                               as city,
    trim(raw_data:pstalp::varchar)                              as state_abbr,
    try_to_date(raw_data:procdate::varchar, 'YYYY-MM-DD')       as process_date,
    trim(raw_data:action::varchar)                              as action_type,
    loaded_at
from source
where raw_data:cert is not null
