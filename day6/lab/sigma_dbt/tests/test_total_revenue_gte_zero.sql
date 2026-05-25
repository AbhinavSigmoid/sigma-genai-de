SELECT
    merchant_id,
    total_revenue
FROM {{ ref('mart_merchant_performance') }}
WHERE total_revenue < 0
