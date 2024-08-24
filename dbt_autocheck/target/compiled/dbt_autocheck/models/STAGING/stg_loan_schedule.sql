SELECT 
    loan_id,
    schedule_id,
    try_cast(expected_payment_date as date) as expected_payment_date,
    expected_payment_amount
FROM AUTOCHECK.RAW.schedule_data