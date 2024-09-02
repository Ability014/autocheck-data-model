
  create or replace   view AUTOCHECK.STAGING.stg_borrowers
  
   as (
    SELECT 
    borrower_id,
    state,
    city,
    zip_code,
    borrower_credit_score
FROM AUTOCHECK.RAW.borrower_data
  );

