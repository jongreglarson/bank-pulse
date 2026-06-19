with latest_per_bank as (
    select
        cert,
        state_name,
        is_active,
        row_number() over (partition by cert order by report_date desc)             as rn
    from {{ ref('stg_institutions') }}
)

select
    state_name,
    count(distinct cert)                                                            as total_institutions,
    sum(case when is_active then 1 else 0 end)                                     as active_institutions
from latest_per_bank
where rn = 1
  and state_name is not null
group by state_name
order by state_name
