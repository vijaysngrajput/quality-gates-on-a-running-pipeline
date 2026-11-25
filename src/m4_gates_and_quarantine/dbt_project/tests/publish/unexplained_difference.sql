-- The receipts add up to the till's own total, or we can say why not
--
-- Prepare first: The model store_day_status: for every open store and day, the lines that are left, the tills' total, and what was set aside
-- The limit:     Where the two totals differ, a missing file, a refused file or a row set aside explains it
-- When it fails: BLOCK: publish does not run. The outputs keep their last good numbers.
--                A difference nobody can explain means a fault no check knows. Do not publish it.
{{ config(tags=['publish'], severity='error', meta={'number': 11, 'action': 'block', 'lens': 'business', 'question': 'Accurate?', 'protects': 'Store managers and the head of retail operations. It is the only check that can say a whole day of money is right'}) }}

SELECT store_id, sales_date, lines_cents, till_cents, lines_cents - till_cents AS difference_cents
FROM {{ ref('store_day_status') }}
WHERE problem = 'unexplained'
