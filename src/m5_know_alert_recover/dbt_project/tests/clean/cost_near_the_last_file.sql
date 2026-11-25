-- A cost is near the cost in the last file
--
-- Prepare first: Join each row to the same supplier and product in the file before
-- The limit:     A cost is at most 2 times the last one, and at least half of it
-- When it fails: QUARANTINE: the rows this test returns are set aside with this reason. The good rows carry on.
{{ config(tags=['clean'], severity='warn', meta={'number': 31, 'action': 'quarantine', 'lens': 'business', 'question': 'Accurate?', 'protects': 'Finance: they pay suppliers from this price. A cost that is 100 times too high is still a valid number', 'sets_aside': 'supplier_prices'}) }}

SELECT new.supplier_id || ' ' || new.product_id AS thing, new.price_date AS day,
       new.supplier_id, new.product_id,
       old.unit_cost_cents AS last_cost_cents, new.unit_cost_cents AS new_cost_cents
FROM {{ source('clean', 'supplier_prices') }} new
JOIN {{ source('clean', 'supplier_prices') }} old
  ON old.supplier_id = new.supplier_id AND old.product_id = new.product_id
 AND old.price_date = (
     SELECT max(before.price_date) FROM {{ source('clean', 'supplier_prices') }} before
     WHERE before.supplier_id = new.supplier_id AND before.product_id = new.product_id
       AND before.price_date < new.price_date AND before.unit_cost_cents IS NOT NULL
 )
WHERE new.unit_cost_cents > 2 * old.unit_cost_cents
   OR new.unit_cost_cents * 2 < old.unit_cost_cents
