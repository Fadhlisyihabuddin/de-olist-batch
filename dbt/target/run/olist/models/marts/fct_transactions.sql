
  
    

  create  table "olist"."public_marts"."fct_transactions__dbt_tmp"
  
  
    as
  
  (
    select
  order_id, customer_id, order_status, payment_type,
  payment_value,
  -- flag sederhana ala fraud/risk: cicilan jumbo / voucher besar
  ((coalesce(installments, 0) >= 10 or (payment_type = 'voucher' and payment_value > 500))) as is_risky,
  date_trunc('day', purchased_at)::date as txn_date
from "olist"."public_intermediate"."int_transactions"
  );
  