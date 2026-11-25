-- Every file is from a store we know
--
-- Prepare first: Collect the store_id of every sales line, till total and stock count, and join them to stores
-- The limit:     Every store_id in the three store files is in the stores table
-- When it fails: BLOCK: the tasks after this gate do not run. The outputs keep their last good numbers.
{{ config(tags=['clean'], severity='error', meta={'number': 24, 'action': 'block', 'lens': 'data', 'question': 'Consistent?', 'protects': 'Everyone who reads the two outputs: a sales line is joined to this table. Sales of a store that is not in this table are dropped by the join'}) }}

SELECT used.store_id AS thing, (SELECT CAST(max(_loaded_at) AS DATE) - 1 FROM {{ source('raw', 'sales_lines') }}) AS day, used.found_in
FROM (
    SELECT DISTINCT store_id, 'sales_lines' AS found_in FROM {{ source('raw', 'sales_lines') }}
    UNION
    SELECT DISTINCT store_id, 'till_totals' FROM {{ source('raw', 'till_totals') }}
    UNION
    SELECT DISTINCT store_id, 'stock_counts' FROM {{ source('raw', 'stock_counts') }}
) used
LEFT JOIN {{ source('clean', 'stores') }} s ON s.store_id = used.store_id
WHERE s.store_id IS NULL
