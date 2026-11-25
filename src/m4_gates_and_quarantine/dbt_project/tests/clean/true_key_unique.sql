-- One item line is stored once
--
-- Prepare first: Number the copies of each receipt_id + line_no. The first copy is number 1
-- The limit:     Each receipt_id + line_no is there exactly once
-- When it fails: QUARANTINE: the extra copies are set aside with this reason. The first copy carries on.
{{ config(tags=['clean'], severity='warn', meta={'number': 2, 'action': 'quarantine', 'lens': 'data', 'question': 'Unique?', 'protects': 'Store managers and the head of retail operations: net sales, so pay', 'sets_aside': 'sales_lines', 'is_a_copy': true}) }}

SELECT line_id, store_id, sales_date, receipt_id, line_no, amount_cents, copy_number
FROM (
    SELECT line_id, store_id, sales_date, receipt_id, line_no, amount_cents,
           row_number() OVER (PARTITION BY receipt_id, line_no ORDER BY line_id) AS copy_number
    FROM {{ source('clean', 'sales_lines') }}
) copies
WHERE copy_number > 1
