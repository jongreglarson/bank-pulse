select
    cert,
    institution_name,
    city,
    state_name,
    is_active,
    report_date,
    fiscal_quarter,
    total_assets_thousands,
    total_deposits_thousands,
    net_income_thousands,
    return_on_assets,
    deposit_to_asset_ratio
from {{ ref('int_bank_financials') }}
