
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select payment_value
from "olist"."public_marts"."fct_transactions"
where payment_value is null



  
  
      
    ) dbt_internal_test