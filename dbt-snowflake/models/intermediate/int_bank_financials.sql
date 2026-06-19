with institutions as (
    select * from {{ ref('stg_institutions') }}
)

select
    cert,
    institution_name,
    city,
    state_name,
    is_active,
    report_date,
    {{ fiscal_quarter('report_date') }}                                               as fiscal_quarter,
    total_assets_thousands,
    total_deposits_thousands,
    net_income_thousands,
    {{ safe_divide('net_income_thousands * 1.0', 'total_assets_thousands') }}         as return_on_assets,
    {{ safe_divide('total_deposits_thousands * 1.0', 'total_assets_thousands') }}     as deposit_to_asset_ratio
from institutions
where total_assets_thousands is not null
