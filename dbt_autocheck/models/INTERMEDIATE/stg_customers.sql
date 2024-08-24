
SELECT 
ID As customer_id,
NAME As customer_name,
_AIRBYTE_EXTRACTED_AT
FROM {{ source('jaffle_shop', 'raw_customers') }}