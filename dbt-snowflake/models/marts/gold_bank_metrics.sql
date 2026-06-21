with silver as (
    select * from {{ ref('stg_financials') }}
),

with_metrics as (
    select
        *,
        interest_income - interest_expense as net_interest_income,
        case
            when total_assets > 0
            then (interest_income - interest_expense) / total_assets * 100
                 * (4.0 / quarter(report_date))
        end as nim,
        nonperforming_assets as npl_ratio,
        case
            when total_assets < 100000    then 'Community (<$100M)'
            when total_assets < 1000000   then 'Mid-Size ($100M-$1B)'
            when total_assets < 10000000  then 'Regional ($1B-$10B)'
            when total_assets < 100000000 then 'Large ($10B-$100B)'
            else                               'Mega (>$100B)'
        end as peer_group,
        case
            when total_assets < 100000    then 1
            when total_assets < 1000000   then 2
            when total_assets < 10000000  then 3
            when total_assets < 100000000 then 4
            else                               5
        end as peer_group_sort
    from silver
),

with_qoq as (
    select
        *,
        lag(total_assets) over (partition by cert order by report_date) as assets_prior_qtr,
        lag(nim)          over (partition by cert order by report_date) as nim_prior_qtr,
        lag(npl_ratio)    over (partition by cert order by report_date) as npl_ratio_prior_qtr,
        lag(roa)          over (partition by cert order by report_date) as roa_prior_qtr
    from with_metrics
)

select
    cert,
    report_date,
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
    source,
    net_interest_income,
    nim,
    npl_ratio,
    peer_group,
    peer_group_sort,
    case
        when assets_prior_qtr > 0
        then (total_assets - assets_prior_qtr) / assets_prior_qtr * 100
    end                                     as assets_qoq_chg,
    nim - nim_prior_qtr                     as nim_qoq_chg,
    npl_ratio - npl_ratio_prior_qtr         as npl_ratio_qoq_chg,
    roa - roa_prior_qtr                     as roa_qoq_chg
from with_qoq
