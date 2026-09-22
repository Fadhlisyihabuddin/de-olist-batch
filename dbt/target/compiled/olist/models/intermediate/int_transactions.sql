with payments_agg as (
    select
        order_id,
        sum(payment_value) as payment_value,
        max(installments) as installments,
        max(payment_type) as payment_type
    from "olist"."public_staging"."stg_payments"
    group by order_id
)
select
  o.order_id, o.customer_id, o.order_status, o.purchased_at,
  p.payment_type, p.installments,
  coalesce(p.payment_value, 0) as payment_value,
  o.ds
from "olist"."public_staging"."stg_orders" o
left join payments_agg p using (order_id)