SELECT
    b.*,
    loan_id,
    date_of_release,
    term,
    interest_rate,
    loan_amount,
    down_payment,
    payment_frequency,
    maturity_date
FROM AUTOCHECK.RAW.stg_borrowers as b 
LEFT JOIN AUTOCHECK.RAW.stg_loans as l 
on b.borrower_id = l.borrower_id