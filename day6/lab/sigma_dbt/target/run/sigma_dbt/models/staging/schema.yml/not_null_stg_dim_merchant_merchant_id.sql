
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



with __dbt__cte__stg_dim_merchant as (
WITH source AS (
    SELECT * FROM SIGMA_DE.PUBLIC.dim_merchant
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
) select merchant_id
from __dbt__cte__stg_dim_merchant
where merchant_id is null



  
  
      
    ) dbt_internal_test