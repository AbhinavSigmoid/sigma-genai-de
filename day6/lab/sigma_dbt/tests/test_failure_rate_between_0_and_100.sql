SELECT
    merchant_id,
    failure_rate_pct
FROM {{ ref('mart_merchant_performance') }}
WHERE failure_rate_pct < 0 OR failure_rate_pct > 100
