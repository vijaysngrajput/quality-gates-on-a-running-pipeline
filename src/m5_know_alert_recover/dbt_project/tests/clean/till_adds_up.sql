-- A till's total adds up
--
-- Prepare first: Nothing. The three numbers are on one row
-- The limit:     net_total_cents is gross_sales_cents less refunds_cents. No number is below zero
-- When it fails: BLOCK: the tasks after this gate do not run. The outputs keep their last good numbers.
{{ config(tags=['clean'], severity='error', meta={'number': 15, 'action': 'block', 'lens': 'business', 'question': 'Consistent?', 'protects': 'Everyone who is protected by the till total check: it is only as good as this table'}) }}

SELECT store_id, till_id, business_date AS sales_date,
       gross_sales_cents, refunds_cents, net_total_cents
FROM {{ source('clean', 'till_totals') }}
WHERE net_total_cents <> gross_sales_cents - refunds_cents
   OR gross_sales_cents < 0 OR refunds_cents < 0
   OR net_total_cents IS NULL OR gross_sales_cents IS NULL OR refunds_cents IS NULL
