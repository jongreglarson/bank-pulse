with source as (
    select * from {{ source('bronze', 'failures') }}
)

select
    raw_data:cert::int                                          as cert,
    trim(raw_data:name::varchar)                                as institution_name,
    try_to_date(raw_data:faildate::varchar, 'YYYY-MM-DD')       as failure_date,
    trim(raw_data:savr::varchar)                                as savings_rate_class,
    trim(raw_data:restype::varchar)                             as resolution_type,
    raw_data:cost::float                                        as estimated_loss_millions,
    raw_data:qbfdep::bigint                                     as deposits_at_failure_thousands,
    raw_data:asset::bigint                                      as assets_at_failure_thousands,
    loaded_at
from source
where raw_data:cert is not null
