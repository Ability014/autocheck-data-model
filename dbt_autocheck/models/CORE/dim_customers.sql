

SELECT 
{{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
*
FROM {{ ref('stg_customers') }}