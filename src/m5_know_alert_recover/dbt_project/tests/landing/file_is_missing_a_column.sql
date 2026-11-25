-- A file has every column we expect
--
-- Prepare first: Keep the first line of every file. Put the columns we expect beside the columns each file really had
-- The limit:     No expected column is missing
-- When it fails: REFUSE THE FILE: none of its rows go on. The last good file stays in use.
{{ config(tags=['landing'], severity='warn', meta={'number': 29, 'action': 'refuse_file', 'lens': 'data', 'question': 'Valid?', 'protects': 'Everyone who reads that table. A renamed column loads as an empty column, and nothing fails'}) }}

SELECT f.file_name, f.table_name, e.column_name AS missing_column
FROM (SELECT DISTINCT file_name, table_name FROM {{ source('raw', 'file_columns') }}) f
JOIN {{ ref('expected_columns') }} e ON e.table_name = f.table_name
LEFT JOIN {{ source('raw', 'file_columns') }} found
  ON found.file_name = f.file_name AND found.column_name = e.column_name
WHERE found.column_name IS NULL
