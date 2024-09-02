
    
    

select
    schedule_payments_key as unique_field,
    count(*) as n_records

from AUTOCHECK.RAW.fct_repayments
where schedule_payments_key is not null
group by schedule_payments_key
having count(*) > 1


