-- A product costs less than it sells for
--
-- Prepare first: Join each cost to the product's shelf price valid on that day
-- The limit:     unit_cost_cents is below unit_price_cents
-- When it fails: QUARANTINE: the rows this test returns are set aside with this reason. The good rows carry on.
{{ config(tags=['clean'], severity='warn', meta={'number': 32, 'action': 'quarantine', 'lens': 'business', 'question': 'Accurate?', 'protects': 'Finance: they pay suppliers from this price', 'sets_aside': 'supplier_prices'}) }}

SELECT s.supplier_id || ' ' || s.product_id AS thing, s.price_date AS day,
       s.supplier_id, s.product_id, p.product_name,
       s.unit_cost_cents, p.unit_price_cents AS shelf_price_cents
FROM {{ source('clean', 'supplier_prices') }} s
JOIN {{ source('clean', 'products') }} p
  ON p.product_id = s.product_id
 AND s.price_date BETWEEN p.valid_from AND coalesce(p.valid_to, DATE '9999-12-31')
WHERE s.unit_cost_cents >= p.unit_price_cents
