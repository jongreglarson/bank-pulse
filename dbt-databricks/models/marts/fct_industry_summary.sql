select
    report_date,
    {{ fiscal_quarter('report_date') }}                                                     as fiscal_quarter,
    total_assets_thousands,
    total_deposits_thousands,
    interest_income_thousands,
    noninterest_income_thousands,
    net_income_thousands,
    net_loans_thousands,
    {{ safe_divide('net_income_thousands * 1.0', 'total_assets_thousands') }}               as industry_roa,
    {{ safe_divide('net_loans_thousands * 1.0', 'total_deposits_thousands') }}              as industry_loan_to_deposit_ratio
from {{ ref('stg_summary') }}
where report_date is not null
order by report_date
