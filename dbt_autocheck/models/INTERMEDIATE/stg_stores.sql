

SELECT
ID as store_id,
NAME As store_name,
tax_rate,
CAST(opened_at as DATE) As opened_at,
_AIRBYTE_EXTRACTED_AT
FROM {{ source('jaffle_shop', 'raw_stores') }}