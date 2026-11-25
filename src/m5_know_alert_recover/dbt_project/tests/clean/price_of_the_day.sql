-- The price charged is the price valid that day
--
-- Prepare first: Join each line to the product's price row that is valid on the day of the sale
-- The limit:     unit_price_cents equals the price list, exactly
-- When it fails: WARN: the run continues, and the warning is recorded.
{{ config(tags=['clean'], severity='warn', meta={'number': 8, 'action': 'warn', 'lens': 'business', 'question': 'Accurate?', 'protects': 'Finance and the store: money the store should have taken and did not'}) }}

SELECT l.line_id, l.store_id, l.sales_date, l.product_name, l.quantity,
       l.unit_price_cents AS charged_cents, p.unit_price_cents AS price_list_cents,
       CAST(round(l.quantity * (p.unit_price_cents - l.unit_price_cents)) AS BIGINT)
           AS store_short_cents
FROM {{ source('clean', 'sales_lines') }} l
JOIN {{ source('clean', 'products') }} p
  ON p.product_id = l.product_id
 AND l.sales_date BETWEEN p.valid_from AND coalesce(p.valid_to, DATE '9999-12-31')
WHERE l.unit_price_cents <> p.unit_price_cents
