-- Standard check 1 of milestone 1: the table is not empty
--
-- It is a SINGULAR test: one query in one file, for one table. One row comes back when
-- the newest loaded day has no sales lines at all.
-- When it fails: BLOCK.
{{ config(tags=['clean'], severity='error', meta={'number': 101, 'action': 'block', 'lens': 'data', 'question': 'Complete?', 'protects': ''}) }}

SELECT 'no sales lines for the newest day' AS problem
FROM (SELECT 1 AS one) nothing
WHERE NOT EXISTS (
    SELECT 1 FROM {{ source('clean', 'sales_lines') }}
    WHERE sales_date = (SELECT CAST(max(_loaded_at) AS DATE) - 1 FROM {{ source('raw', 'sales_lines') }})
)
