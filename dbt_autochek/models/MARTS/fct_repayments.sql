
SELECT
    {{ dbt_utils.generate_surrogate_key(['br.schedule_id', 'br.payment_id']) }} as schedule_payments_key,
    {{ dbt_utils.generate_surrogate_key(['loan_id']) }} as loan_key,
    TRY_CAST(CONCAT(LEFT(date_paid, 4), SUBSTR(date_paid, 6, 2), SUBSTR(date_paid, 9, 2)) As INTEGER) As date_key,
    TRY_CAST(CONCAT(LEFT(date_paid, 4), SUBSTR(date_paid, 6, 2)) As INTEGER) As month_key,
    br.*
from {{ ref('int_borrower_repayments') }} as br