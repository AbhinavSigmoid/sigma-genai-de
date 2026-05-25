WITH source AS (
    SELECT * FROM SIGMA_DE.PUBLIC.dim_customer
),

renamed AS (
    SELECT
        LOWER(customer_id) AS customer_id,
        customer_name,
        email,
        tier,
        signup_date,
        city
    FROM source
)

SELECT * FROM renamed