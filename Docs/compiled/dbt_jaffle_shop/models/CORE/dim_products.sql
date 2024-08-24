SELECT 
md5(cast(coalesce(cast(product_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as product_key,
*
FROM JAFFLE_SHOP.INTERMEDIATE.stg_products