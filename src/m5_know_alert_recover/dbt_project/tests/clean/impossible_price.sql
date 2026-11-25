-- A price is not impossible
--
-- Prepare first: Join each line to the product's price row that is valid on the day of the sale
-- The limit:     The price charged is at most twice the price list, and at least half of it
-- When it fails: QUARANTINE: the rows this test returns are set aside with this reason. The good rows carry on.
--
-- "The price charged is the price valid that day" only warns: a store that charges yesterday's
-- price took real money. A price 100 times the list is not a price. It is a broken file.
{{ config(tags=['clean'], severity='warn', meta={'number': 43, 'action': 'quarantine', 'lens': 'business', 'question': 'Valid?', 'protects': 'Store managers and the head of retail operations: net sales, so pay', 'sets_aside': 'sales_lines'}) }}

SELECT l.line_id, l.store_id, l.sales_date, l.product_name,
       l.unit_price_cents AS charged_cents, p.unit_price_cents AS price_list_cents
FROM {{ source('clean', 'sales_lines') }} l
JOIN {{ source('clean', 'products') }} p
  ON p.product_id = l.product_id
 AND l.sales_date BETWEEN p.valid_from AND coalesce(p.valid_to, DATE '9999-12-31')
WHERE l.unit_price_cents > 2 * p.unit_price_cents
   OR l.unit_price_cents * 2 < p.unit_price_cents
