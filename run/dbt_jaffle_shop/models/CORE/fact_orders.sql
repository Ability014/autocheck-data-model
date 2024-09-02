
  
    

        create or replace  table JAFFLE_SHOP.CORE.fact_orders
         as
        (SELECT
    md5(cast(coalesce(cast(orders.order_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(orders.item_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as orders_key,
    md5(cast(coalesce(cast(customer_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as customer_key,
    md5(cast(coalesce(cast(product_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as product_key,
    md5(cast(coalesce(cast(store_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as store_key,
    TRY_CAST(CONCAT(LEFT(ordered_date, 4), SUBSTR(ordered_date, 6, 2), SUBSTR(ordered_date, 9, 2)) As INTEGER) As date_key,
    TRY_CAST(CONCAT(LEFT(ordered_date, 4), SUBSTR(ordered_date, 6, 2)) As INTEGER) As month_key, -- New change
    orders.order_id as salesorderid,
    item_id as salesitemid,
    subtotal,
    tax_paid,
    order_total,
    ordered_date
from JAFFLE_SHOP.INTERMEDIATE.stg_salesorders as orders
        );
      
  