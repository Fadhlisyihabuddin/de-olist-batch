select
  order_id::text as order_id,
  payment_type::text as payment_type,
  payment_installments::int as installments,
  payment_value::numeric as payment_value,
  ds::date as ds
from raw.raw_payments
where order_id is not null