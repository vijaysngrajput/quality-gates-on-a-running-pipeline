-- A file has no column we did not expect
--
-- Prepare first: Keep the first line of every file
-- The limit:     Every column of a file is an expected column
-- When it fails: WARN: the run continues, and the warning is recorded. A new column breaks no reader.
{{ config(tags=['landing'], severity='warn', meta={'number': 38, 'action': 'warn', 'lens': 'data', 'question': 'Valid?', 'protects': 'Nobody is hurt today. It is news: the sender changed something'}) }}

SELECT found.file_name, found.table_name, found.column_name AS new_column
FROM {{ source('raw', 'file_columns') }} found
LEFT JOIN {{ ref('expected_columns') }} e
  ON e.table_name = found.table_name AND e.column_name = found.column_name
WHERE e.column_name IS NULL
