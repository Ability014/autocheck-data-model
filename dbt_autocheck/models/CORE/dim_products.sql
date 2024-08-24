
SELECT 
{{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
*
FROM {{ ref('stg_products') }}