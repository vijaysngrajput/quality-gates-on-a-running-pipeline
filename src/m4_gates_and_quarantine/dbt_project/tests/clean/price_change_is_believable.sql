-- A new price is near the old price
--
-- Prepare first: Join each price row to the row that ended the day before
-- The limit:     A new price is at most 2 times the old one, and at least half of it
-- When it fails: WARN: the run continues, and the warning is recorded.
{{ config(tags=['clean'], severity='warn', meta={'number': 20, 'action': 'warn', 'lens': 'business', 'question': 'Accurate?', 'protects': 'Customers and store managers: a typing mistake in a price is charged at every till'}) }}

SELECT new.product_id AS thing, new.valid_from AS day, new.product_name,
       old.unit_price_cents AS old_price_cents, new.unit_price_cents AS new_price_cents
FROM {{ source('clean', 'products') }} new
JOIN {{ source('clean', 'products') }} old
  ON old.product_id = new.product_id AND old.valid_to + 1 = new.valid_from
WHERE new.unit_price_cents > 2 * old.unit_price_cents
   OR new.unit_price_cents * 2 < old.unit_price_cents
