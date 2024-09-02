
  create or replace   view FIRST_DE_PROJ.RAW.my_second_dbt_model
  
   as (
    -- Use the `ref` function to select from other models

select *
from FIRST_DE_PROJ.RAW.my_first_dbt_model
where id = 1
  );

