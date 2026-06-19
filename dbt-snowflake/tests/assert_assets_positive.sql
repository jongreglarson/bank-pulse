-- Singular test: all banks must have positive total assets.
-- Any rows returned by this query are test failures.
select
    cert,
    report_date,
    total_assets_thousands
from {{ ref('fct_bank_financials') }}
where total_assets_thousands is not null
  and total_assets_thousands <= 0
