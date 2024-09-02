select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select customer as from_field
    from JAFFLE_SHOP.RAW.raw_orders
    where customer is not null
),

parent as (
    select id as to_field
    from JAFFLE_SHOP.RAW.raw_customers
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test