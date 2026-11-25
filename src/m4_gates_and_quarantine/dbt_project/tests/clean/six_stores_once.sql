-- There are six stores, each once
--
-- Prepare first: Count the rows. For a table this small, the exact count is a fact you can know
-- The limit:     Exactly 6 rows, and no store_id twice
-- When it fails: BLOCK: the tasks after this gate do not run. The outputs keep their last good numbers.
{{ config(tags=['clean'], severity='error', meta={'number': 22, 'action': 'block', 'lens': 'data', 'question': 'Unique?', 'protects': 'Everyone who reads the two outputs: a sales line is joined to this table. A store that is there twice doubles its sales'}) }}

SELECT 'the stores table' AS thing, (SELECT CAST(max(_loaded_at) AS DATE) - 1 FROM {{ source('raw', 'sales_lines') }}) AS day,
       count(*) AS stores_found, 6 AS stores_expected
FROM {{ source('clean', 'stores') }}
HAVING count(*) <> 6
UNION ALL
SELECT store_id AS thing, (SELECT CAST(max(_loaded_at) AS DATE) - 1 FROM {{ source('raw', 'sales_lines') }}) AS day, count(*) AS stores_found, 1 AS stores_expected
FROM {{ source('clean', 'stores') }}
GROUP BY store_id
HAVING count(*) > 1
