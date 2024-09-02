SELECT
    md5(cast(coalesce(cast(br.schedule_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(br.payment_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as schedule_payments_key,
    md5(cast(coalesce(cast(loan_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as loan_key,
    TRY_CAST(CONCAT(LEFT(date_paid, 4), SUBSTR(date_paid, 6, 2), SUBSTR(date_paid, 9, 2)) As INTEGER) As date_key,
    TRY_CAST(CONCAT(LEFT(date_paid, 4), SUBSTR(date_paid, 6, 2)) As INTEGER) As month_key,
    br.*
from AUTOCHECK.RAW.int_borrower_repayments as br