-- A line adds up
--
-- Prepare first: Work the amount out again from the line's own quantity and price
-- The limit:     amount_cents is quantity x unit_price_cents, to the cent
-- When it fails: QUARANTINE: the rows this test returns are set aside with this reason. The good rows carry on.
{{ config(tags=['clean'], severity='warn', meta={'number': 4, 'action': 'quarantine', 'lens': 'data', 'question': 'Consistent?', 'protects': 'Store managers and the head of retail operations: net sales, so pay', 'sets_aside': 'sales_lines'}) }}

SELECT line_id, store_id, sales_date, quantity, unit_price_cents, amount_cents,
       CAST(round(quantity * unit_price_cents) AS BIGINT) AS should_be_cents
FROM {{ source('clean', 'sales_lines') }}
WHERE amount_cents <> CAST(round(quantity * unit_price_cents) AS BIGINT)
