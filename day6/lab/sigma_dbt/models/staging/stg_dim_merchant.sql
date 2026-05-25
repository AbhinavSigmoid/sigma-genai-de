WITH source AS (
    SELECT * FROM {{ source('sigma_analytics', 'dim_merchant') }}
),

renamed AS (
    SELECT
        LOWER(merchant_id) AS merchant_id,
        merchant_name,
        category,
        city
    FROM source
)

SELECT * FROM renamed
