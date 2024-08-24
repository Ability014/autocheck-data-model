
  
    

        create or replace  table JAFFLE_SHOP.CORE.dim_date
         as
        (SELECT
TRY_CAST(CONCAT(LEFT(DATE_DAY, 4), SUBSTR(DATE_DAY, 6, 2), SUBSTR(DATE_DAY, 9, 2)) AS INTEGER) As date_key,
TRY_CAST(CONCAT(LEFT(DATE_DAY, 4), SUBSTR(DATE_DAY, 6, 2)) AS INTEGER) As month_key,
*
FROM JAFFLE_SHOP.RAW.dates
        );
      
  