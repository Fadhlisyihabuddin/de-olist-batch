
    
    

select
    order_id as unique_field,
    count(*) as n_records

from "olist"."public_marts"."fct_transactions"
where order_id is not null
group by order_id
having count(*) > 1


