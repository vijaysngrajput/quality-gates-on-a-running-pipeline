-- Can this store's day be paid from? One row per open store per day.
--
-- It is built AFTER the bad rows were set aside. So `lines_cents` is what is left.
--
--   status    ok, or do not pay yet
--   problem   why not:
--     no_sales_file    the store sent nothing
--     file_refused     the file was refused when it landed
--     rows_set_aside   bad rows were set aside, and the day no longer adds up to the tills
--     no_till_total    there is nothing to compare with
--     unexplained      the two counts differ, and nothing above explains it
WITH lines AS (
    SELECT store_id, sales_date, sum(amount_cents) AS lines_cents, count(*) AS lines
    FROM {{ source('clean', 'sales_lines') }}
    GROUP BY store_id, sales_date
),

tills AS (
    SELECT store_id, sales_date, sum(till_cents) AS till_cents
    FROM {{ ref('till_reconciliation') }}
    GROUP BY store_id, sales_date
),

-- One row per line that was set aside, however many reasons it has.
aside_lines AS (
    SELECT DISTINCT store_id, file_day, line_id, amount_cents
    FROM {{ source('quarantine', 'sales_lines') }}
    UNION
    SELECT DISTINCT store_id, file_day, line_id, amount_cents
    FROM {{ source('quarantine', 'raw_sales_lines') }}
),

aside AS (
    SELECT store_id, file_day, count(*) AS rows_set_aside, sum(amount_cents) AS cents_set_aside
    FROM aside_lines
    GROUP BY store_id, file_day
),

reasons AS (
    SELECT store_id, file_day, string_agg(DISTINCT reason, '; ' ORDER BY reason) AS reasons
    FROM (
        SELECT store_id, file_day, reason FROM {{ source('quarantine', 'sales_lines') }}
        UNION ALL
        SELECT store_id, file_day, reason FROM {{ source('quarantine', 'raw_sales_lines') }}
    ) every_reason
    GROUP BY store_id, file_day
),

refused AS (
    SELECT substr(file_name, 13, 3) AS store_id, CAST(substr(file_name, 17, 10) AS DATE) AS file_day,
           min(reason) AS reason
    FROM {{ source('ops', 'refused_files') }}
    WHERE table_name = 'sales_lines'
    GROUP BY substr(file_name, 13, 3), CAST(substr(file_name, 17, 10) AS DATE)
),

judged AS (
    SELECT e.store_id,
           e.day AS sales_date,
           coalesce(lines.lines_cents, 0) AS lines_cents,
           tills.till_cents,
           coalesce(aside.rows_set_aside, 0) AS rows_set_aside,
           coalesce(aside.cents_set_aside, 0) AS cents_set_aside,
           coalesce(refused.reason, reasons.reasons) AS reasons,
           CASE
               WHEN tills.till_cents IS NULL THEN 'no_till_total'
               WHEN coalesce(lines.lines_cents, 0) = tills.till_cents THEN NULL
               WHEN refused.store_id IS NOT NULL THEN 'file_refused'
               WHEN lines.store_id IS NULL AND aside.store_id IS NULL THEN 'no_sales_file'
               WHEN aside.store_id IS NOT NULL THEN 'rows_set_aside'
               ELSE 'unexplained'
           END AS problem
    FROM {{ ref('expected_store_days') }} e
    LEFT JOIN lines ON lines.store_id = e.store_id AND lines.sales_date = e.day
    LEFT JOIN tills ON tills.store_id = e.store_id AND tills.sales_date = e.day
    LEFT JOIN aside ON aside.store_id = e.store_id AND aside.file_day = e.day
    LEFT JOIN reasons ON reasons.store_id = e.store_id AND reasons.file_day = e.day
    LEFT JOIN refused ON refused.store_id = e.store_id AND refused.file_day = e.day
)

SELECT store_id, sales_date, lines_cents, till_cents, rows_set_aside, cents_set_aside, reasons,
       problem,
       CASE WHEN problem IS NULL THEN 'ok' ELSE 'do not pay yet' END AS status
FROM judged
