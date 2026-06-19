with source as (
    select * from {{ source('bronze', 'failures') }}
)

select
    cast(cert as int)                                           as cert,
    trim(name)                                                  as institution_name,
    to_date(cast(faildate as string), 'yyyy-MM-dd')             as failure_date,
    trim(savr)                                                  as savings_rate_class,
    trim(restype)                                               as resolution_type,
    cast(cost as double)                                        as estimated_loss_millions,
    cast(qbfdep as bigint)                                      as deposits_at_failure_thousands,
    cast(asset as bigint)                                       as assets_at_failure_thousands,
    _loaded_at
from source
where cert is not null
