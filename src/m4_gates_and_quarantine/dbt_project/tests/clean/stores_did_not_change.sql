-- The stores table is the same as yesterday
--
-- Prepare first: Take the newest file and the file before it, and compare them row by row
-- The limit:     No row is new, gone or different. A real change is confirmed by a person
-- When it fails: WARN: the run continues, and the warning is recorded.
{{ config(tags=['clean'], severity='warn', meta={'number': 25, 'action': 'warn', 'lens': 'operational', 'question': 'Consistent?', 'protects': 'Everyone who reads the two outputs: a sales line is joined to this table. This table is typed once and rarely changed, so any change is news'}) }}

SELECT store_id AS thing, (SELECT CAST(max(_loaded_at) AS DATE) - 1 FROM {{ source('raw', 'sales_lines') }}) AS day, store_name, opens_at, closes_at,
       manager_name, tills
FROM (
    SELECT store_id, store_name, town, opens_at, closes_at, manager_name, tills FROM {{ source('raw', 'stores') }}
    WHERE _file_name = (SELECT max(_file_name) FROM {{ source('raw', 'stores') }})
    EXCEPT
    SELECT store_id, store_name, town, opens_at, closes_at, manager_name, tills FROM {{ source('raw', 'stores') }}
    WHERE _file_name = (
        SELECT max(_file_name) FROM {{ source('raw', 'stores') }}
        WHERE _file_name < (SELECT max(_file_name) FROM {{ source('raw', 'stores') }})
    )
) changed
