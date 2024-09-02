WITH indexed_schedules as (
    SELECT
        *,
        SUM(expected_payment_amount) OVER(PARTITION BY loan_id ORDER BY expected_payment_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as accumulated_payment_schedule,
        ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY expected_payment_date) as row_id
    FROM AUTOCHECK.RAW.stg_loan_schedule
),
indexed_repayments as (
    SELECT
        *,
        SUM(amount_paid) OVER(PARTITION BY loan_id ORDER BY date_paid ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as accumulated_amount_repaid,
        ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY date_paid) as row_id
    FROM AUTOCHECK.RAW.stg_loan_repayments
)
SELECT
    s.*,
    payment_id,
    date_paid,
    amount_paid,
    DATEDIFF(day, expected_payment_date, date_paid) as par_days,
    accumulated_payment_schedule - accumulated_amount_repaid as par_amount
FROM indexed_schedules as s
LEFT JOIN indexed_repayments as l 
ON s.loan_id = l.loan_id AND s.row_id = l.row_id