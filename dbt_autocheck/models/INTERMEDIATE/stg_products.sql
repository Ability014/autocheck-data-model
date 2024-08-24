

SELECT
SKU As product_id,
NAME As product_name,
TYPE As product_type,
PRICE As unit_price,
DESCRIPTION As description,
_AIRBYTE_EXTRACTED_AT
FROM {{ source('jaffle_shop', 'raw_products') }}