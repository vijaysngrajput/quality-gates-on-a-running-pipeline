-- Every day the pipeline has loaded, for every store that was open that day.
--
-- A check can only return rows that exist. To find a store that sent nothing,
-- the rows we EXPECTED must be built first. This model builds them.
SELECT s.store_id, d.day
FROM {{ source('clean', 'stores') }} s
CROSS JOIN (
    SELECT DISTINCT business_date AS day FROM {{ source('clean', 'till_totals') }}
) d
WHERE NOT EXISTS (
    SELECT 1 FROM {{ source('clean', 'calendar') }} c
    WHERE c.store_id = s.store_id AND c.calendar_date = d.day AND c.kind = 'closed'
)
