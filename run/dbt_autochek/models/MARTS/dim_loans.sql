
  
    

        create or replace  table AUTOCHECK.MART.dim_loans
         as
        (SELECT 
    md5(cast(coalesce(cast(borrower_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(loan_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as borrower_loan_key,
    md5(cast(coalesce(cast(loan_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as loan_key,
    *
FROM AUTOCHECK.INTERMEDIATE.int_borrower_loans
        );
      
  