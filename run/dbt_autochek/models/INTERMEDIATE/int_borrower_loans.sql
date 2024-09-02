
  
    

        create or replace transient table AUTOCHECK.INTERMEDIATE.int_borrower_loans
         as
        (SELECT
    b.*,
    loan_id,
    date_of_release,
    term,
    interest_rate,
    loan_amount,
    down_payment,
    payment_frequency,
    maturity_date
FROM AUTOCHECK.STAGING.stg_borrowers as b 
LEFT JOIN AUTOCHECK.STAGING.stg_loans as l 
on b.borrower_id = l.borrower_id
        );
      
  