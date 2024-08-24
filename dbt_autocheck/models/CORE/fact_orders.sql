
SELECT
    {{ dbt_utils.generate_surrogate_key(['orders.order_id', 'orders.item_id']) }} as orders_key,
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
    {{ dbt_utils.generate_surrogate_key(['store_id']) }} as store_key,
    TRY_CAST(CONCAT(LEFT(ordered_date, 4), SUBSTR(ordered_date, 6, 2), SUBSTR(ordered_date, 9, 2)) As INTEGER) As date_key,
    TRY_CAST(CONCAT(LEFT(ordered_date, 4), SUBSTR(ordered_date, 6, 2)) As INTEGER) As month_key, -- New change
    orders.order_id as salesorderid,
    item_id as salesitemid,
    subtotal,
    tax_paid,
    order_total,
    ordered_date
from {{ ref('stg_salesorders') }} as orders