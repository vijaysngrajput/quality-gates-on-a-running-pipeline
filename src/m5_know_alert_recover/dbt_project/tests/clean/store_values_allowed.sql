-- A store row makes sense
--
-- Prepare first: Nothing
-- The limit:     A name, a town and a manager. It opens before it closes. It has at least one till
-- When it fails: BLOCK: the tasks after this gate do not run. The outputs keep their last good numbers.
{{ config(tags=['clean'], severity='error', meta={'number': 23, 'action': 'block', 'lens': 'data', 'question': 'Valid?', 'protects': 'Everyone who reads the two outputs: a sales line is joined to this table, and the opening-hours check, which reads these hours'}) }}

SELECT store_id AS thing, (SELECT CAST(max(_loaded_at) AS DATE) - 1 FROM {{ source('raw', 'sales_lines') }}) AS day, store_name, opens_at, closes_at, tills
FROM {{ source('clean', 'stores') }}
WHERE store_name IS NULL OR town IS NULL OR manager_name IS NULL
   OR opens_at IS NULL OR closes_at IS NULL OR opens_at >= closes_at
   OR tills IS NULL OR tills < 1
