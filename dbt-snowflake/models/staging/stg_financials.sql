with source as (
    select * from {{ source('bronze', 'financials') }}
),

typed as (
    select
        try_to_date(raw_data:REPDTE::varchar, 'YYYYMMDD')  as report_date,
        raw_data:CERT::int                                  as cert,
        raw_data:ASSET::bigint                              as total_assets,
        raw_data:DEP::bigint                                as total_deposits,
        raw_data:LNLSNET::bigint                            as net_loans,
        raw_data:INTINC::float                              as interest_income,
        raw_data:EINTEXP::float                             as interest_expense,
        raw_data:NETINC::float                              as net_income,
        raw_data:NPERFV::float                              as nonperforming_assets,
        raw_data:RBC1RWAJ::float                            as tier1_capital_ratio,
        raw_data:ROA::float                                 as roa,
        raw_data:ROE::float                                 as roe,
        loaded_at                                           as ingested_at,
        source_file                                         as source
    from source
    where raw_data:CERT is not null
      and raw_data:REPDTE is not null
),

deduped as (
    select
        *,
        row_number() over (
            partition by cert, report_date
            order by ingested_at desc
        ) as _rank
    from typed
)

select
    report_date,
    cert,
    total_assets,
    total_deposits,
    net_loans,
    interest_income,
    interest_expense,
    net_income,
    nonperforming_assets,
    tier1_capital_ratio,
    roa,
    roe,
    ingested_at,
    source
from deduped
where _rank = 1
