
SELECT 
{{ dbt_utils.generate_surrogate_key(['store_id']) }} as store_key,
*
FROM {{ ref('stg_stores') }}