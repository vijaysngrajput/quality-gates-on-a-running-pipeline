-- A receipt id has the right shape
--
-- Prepare first: Take the id apart: store, till, six digits
-- The limit:     receipt_id is the line's own store, its own till, and six digits: S03-T1-004493
-- When it fails: QUARANTINE: the rows this test returns are set aside with this reason. The good rows carry on.
{{ config(tags=['clean'], severity='warn', meta={'number': 3, 'action': 'quarantine', 'lens': 'data', 'question': 'Valid?', 'protects': 'Finance and the stores: a refund must find the receipt it refunds', 'sets_aside': 'sales_lines'}) }}

SELECT line_id, store_id, sales_date, receipt_id
FROM {{ source('clean', 'sales_lines') }}
WHERE length(receipt_id) <> 13
   OR substr(receipt_id, 1, 7) <> store_id || '-' || till_id || '-'
   OR translate(substr(receipt_id, 8, 6), '0123456789', '') <> ''
