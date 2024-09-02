
SELECT 
    borrower_id,
    state,
    city,
    zip_code,
    borrower_credit_score
FROM {{ source('autochek', 'borrower_data') }}