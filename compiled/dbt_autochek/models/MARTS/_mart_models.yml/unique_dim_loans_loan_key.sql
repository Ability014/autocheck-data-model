
    
    

select
    loan_key as unique_field,
    count(*) as n_records

from AUTOCHECK.MART.dim_loans
where loan_key is not null
group by loan_key
having count(*) > 1


