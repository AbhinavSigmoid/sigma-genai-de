
    
    

with __dbt__cte__stg_dim_customer as (
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
) select
    customer_id as unique_field,
    count(*) as n_records

from __dbt__cte__stg_dim_customer
where customer_id is not null
group by customer_id
having count(*) > 1


