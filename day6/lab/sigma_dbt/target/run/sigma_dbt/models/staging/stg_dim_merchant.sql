
  create or replace   view SIGMA_DE.PUBLIC.stg_dim_merchant
  
  
  
  
  as (
    WITH source AS (
    SELECT * FROM SIGMA_DE.PUBLIC.dim_merchant
),

renamed AS (
    SELECT
        LOWER(merchant_id) AS merchant_id,
        merchant_name,
        category,
        city,
        onboarded_date
    FROM source
)

SELECT * FROM renamed
  );

