
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



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
) select customer_id
from __dbt__cte__stg_dim_customer
where customer_id is null



  
  
      
    ) dbt_internal_test