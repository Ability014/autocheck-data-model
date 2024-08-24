

SELECT 
    {{ dbt_utils.generate_surrogate_key(['borrower_id', 'loan_id']) }} as borrower_loan_key,
    {{ dbt_utils.generate_surrogate_key(['loan_id']) }} as loan_key,
    *
FROM {{ ref('int_borrower_loans') }}