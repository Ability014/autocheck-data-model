SELECT 
    loan_id,
    payment_id,
    try_cast(date_paid as date) as date_paid,
    amount_paid
FROM AUTOCHECK.RAW.repayment_data