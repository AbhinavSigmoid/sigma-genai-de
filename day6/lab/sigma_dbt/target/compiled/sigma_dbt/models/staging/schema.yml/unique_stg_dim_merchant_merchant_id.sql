
    
    

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
) select
    merchant_id as unique_field,
    count(*) as n_records

from __dbt__cte__stg_dim_merchant
where merchant_id is not null
group by merchant_id
having count(*) > 1


