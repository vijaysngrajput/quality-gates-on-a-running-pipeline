-- A store's file arrives before 01:00
--
-- Prepare first: Take the day a file is for from its name. Work out when it was due: 01:00 the next morning
-- The limit:     received_at is not after the moment it was due
-- When it fails: WARN: the run continues, and the warning is recorded. A late file has arrived,
--                so its rows are loaded. But a night ran without it, and that night's numbers were short.
--
-- This is a FRESHNESS check. It cannot see a file that never arrived: a row that is not
-- there cannot be late. "Every store has a day we can pay from" at the publish gate sees that.
{{ config(tags=['landing'], severity='warn', meta={'number': 44, 'action': 'warn', 'lens': 'operational', 'question': 'Fresh?', 'protects': 'The replenishment team, who ordered from a short day. And everyone who read that night\'s report'}) }}

SELECT file_name, substr(file_name, 13, 3) AS store_id, file_day AS sales_date, received_at,
       CAST(file_day + 1 AS TIMESTAMP) + INTERVAL '1 hour' AS due_at
FROM (
    -- Only a store's own files carry their day at this place in the name.
    SELECT file_name, received_at,
           CASE WHEN table_name IN ('sales_lines', 'till_totals')
                THEN CAST(substr(file_name, 17, 10) AS DATE) END AS file_day
    FROM {{ source('raw', 'loaded_files') }}
) files
WHERE file_day IS NOT NULL
  AND received_at > CAST(file_day + 1 AS TIMESTAMP) + INTERVAL '1 hour'
