-- Deliberate test failure for validation
SELECT
    transaction_id,
    status
FROM {{ ref('stg_transactions') }}
WHERE status IN ('pending', 'cancelled')
