-- A retired product has a last day
--
-- Prepare first: Nothing. Both facts are on one row
-- The limit:     status is retired exactly when the last price row has an end date and nothing follows
-- When it fails: WARN: the run continues, and the warning is recorded.
{{ config(tags=['clean'], severity='warn', meta={'number': 21, 'action': 'warn', 'lens': 'business', 'question': 'Consistent?', 'protects': 'The replenishment team: they order from this number. A retired product with no last day is ordered for ever'}) }}

SELECT product_id AS thing, valid_from AS day, product_name, status, valid_to
FROM {{ source('clean', 'products') }}
WHERE status = 'retired' AND valid_to IS NULL
