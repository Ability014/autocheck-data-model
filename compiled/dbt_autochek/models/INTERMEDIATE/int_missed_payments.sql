SELECT 
    loan_id,
    payment_id,
    date_paid,
    amount_paid
FROM AUTOCHECK.INTERMEDIATE.int_borrower_repayments
WHERE par_days > 0