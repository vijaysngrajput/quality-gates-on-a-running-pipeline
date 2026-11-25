-- Every order on the list has a cost
--
-- When it fails: WARN. Finance cannot approve an order with no cost.
{{ config(tags=['publish'], severity='warn', meta={'number': 40, 'action': 'warn', 'lens': 'business', 'question': 'Complete?', 'protects': 'Finance: an order with no cost cannot be approved or paid'}) }}

SELECT store_id, for_date AS sales_date, product_id, order_qty
FROM {{ source('staging', 'replenishment_list') }}
WHERE order_qty > 0 AND unit_cost_cents IS NULL
