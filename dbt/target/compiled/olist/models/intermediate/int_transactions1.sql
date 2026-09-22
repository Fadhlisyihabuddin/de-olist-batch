select
  o.order_id, o.customer_id, o.order_status, o.purchased_at,
  p.payment_type, p.installments, p.payment_value,
  o.ds
from "olist"."public_staging"."stg_orders" o
left join "olist"."public_staging"."stg_payments" p using (order_id)