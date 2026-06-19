with source as (
    select * from {{ source('bronze', 'history') }}
)

select
    cast(cert as int)                                           as cert,
    trim(instname)                                              as institution_name,
    trim(class)                                                 as charter_class,
    trim(pcity)                                                 as city,
    trim(pstalp)                                                as state_abbr,
    to_date(cast(procdate as string), 'yyyy-MM-dd')             as process_date,
    trim(action)                                                as action_type,
    _loaded_at
from source
where cert is not null
