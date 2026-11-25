-- Every open store has a day we can pay from
--
-- Prepare first: The model store_day_status
-- The limit:     The status of each store's day is ok
-- When it fails: WARN: the run continues and the outputs are published. The bonus report marks
--                the store "do not pay yet", with the reason. This is a PARTIAL LOAD: the good
--                rows went on, and the output says what it is missing.
{{ config(tags=['publish'], severity='warn', meta={'number': 12, 'action': 'warn', 'lens': 'operational', 'question': 'Complete?', 'protects': 'Store managers, and the head of retail operations who signs the bonus'}) }}

SELECT store_id, sales_date, problem, rows_set_aside, cents_set_aside, reasons
FROM {{ ref('store_day_status') }}
WHERE status <> 'ok'
