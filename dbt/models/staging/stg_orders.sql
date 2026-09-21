select
  order_id::text as order_id,
  customer_id::text as customer_id,
  order_status::text as order_status,
  order_purchase_timestamp::timestamp as purchased_at,
  ds::date as ds
from raw.raw_orders
where order_id is not null
