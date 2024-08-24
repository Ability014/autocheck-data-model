
SELECT 
    borrower_id,
    loan_id,
    try_cast(date_of_release as date) as date_of_release,
    term,
    interestrate as interest_rate,
    loanamount as loan_amount,
    downpayment as down_payment,
    payment_frequency,
    try_cast(maturity_date as date) as maturity_date
FROM {{ source('autocheck', 'loan_data') }}