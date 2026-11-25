-- Every till of an open store sent its total
--
-- Prepare first: Make the list of tills a day should have: two for every open store
-- The limit:     Each open store has a total for T1 and for T2 on each day
-- When it fails: WARN: the run continues, and the warning is recorded.
{{ config(tags=['clean'], severity='warn', meta={'number': 16, 'action': 'warn', 'lens': 'operational', 'question': 'Complete?', 'protects': 'Everyone who is protected by the till total check: it is only as good as this table. A missing total would make a missing sales file look fine'}) }}

SELECT e.store_id, e.day AS sales_date, count(t.till_id) AS tills_reported
        FROM (
    SELECT s.store_id, d.day
    FROM {{ source('clean', 'stores') }} s
    CROSS JOIN (
        SELECT DISTINCT business_date AS day FROM {{ source('clean', 'till_totals') }}
        UNION
        SELECT DISTINCT sales_date FROM {{ source('clean', 'sales_lines') }} WHERE sales_date <= (SELECT CAST(max(_loaded_at) AS DATE) - 1 FROM {{ source('raw', 'sales_lines') }})
    ) d
    WHERE NOT EXISTS (
        SELECT 1 FROM {{ source('clean', 'calendar') }} c
        WHERE c.store_id = s.store_id AND c.calendar_date = d.day AND c.kind = 'closed'
    )
) e
        LEFT JOIN {{ source('clean', 'till_totals') }} t
          ON t.store_id = e.store_id AND t.business_date = e.day
        GROUP BY e.store_id, e.day
        HAVING count(t.till_id) <> 2
