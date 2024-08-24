
SELECT 
    borrower_id,
    state,
    city,
    zip_code,
    borrower_credit_score
FROM {{ source('autocheck', 'borrower_data') }}