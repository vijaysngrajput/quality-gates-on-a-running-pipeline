-- Every supplier row has a cost
--
-- Prepare first: Nothing
-- The limit:     No empty unit_cost, product, supplier, unit or date
-- When it fails: QUARANTINE: the rows this test returns are set aside with this reason. The good rows carry on.
{{ config(tags=['clean'], severity='warn', meta={'number': 30, 'action': 'quarantine', 'lens': 'data', 'question': 'Complete?', 'protects': 'Finance: they pay suppliers from this price. An order with no cost cannot be approved or paid', 'sets_aside': 'supplier_prices'}) }}

SELECT supplier_id || ' ' || product_id AS thing, price_date AS day,
       supplier_id, product_id, unit_cost_cents
FROM {{ source('clean', 'supplier_prices') }}
WHERE unit_cost_cents IS NULL OR product_id IS NULL OR supplier_id IS NULL
   OR unit IS NULL OR price_date IS NULL
