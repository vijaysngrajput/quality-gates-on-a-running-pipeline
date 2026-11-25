-- A store's day is near its normal day
--
-- Prepare first: Count the lines per store per day. Work out a normal day: the average of the same weekday in the four weeks before. Leave out the days in the calendar
-- The limit:     Between 60% and 140% of the normal day, when at least three earlier days can be compared
-- When it fails: WARN: the run continues, and the warning is recorded.
{{ config(tags=['clean'], severity='warn', meta={'number': 13, 'action': 'warn', 'lens': 'operational', 'question': 'Complete?', 'protects': 'Store managers and the head of retail operations: net sales, so pay'}) }}

SELECT store_id, sales_date, lines, CAST(round(normal_lines) AS INTEGER) AS normal_lines,
               CAST(round(100.0 * lines / normal_lines) AS INTEGER) AS percent_of_normal
        FROM (
    SELECT today.store_id, today.sales_date, today.lines,
           count(before.lines) AS days_compared, avg(before.lines) AS normal_lines
    FROM (
        SELECT store_id, sales_date, count(*) AS lines FROM {{ source('clean', 'sales_lines') }}
        WHERE sales_date <= (SELECT CAST(max(_loaded_at) AS DATE) - 1 FROM {{ source('raw', 'sales_lines') }})
        GROUP BY store_id, sales_date
    ) today
    JOIN (
        SELECT store_id, sales_date, count(*) AS lines FROM {{ source('clean', 'sales_lines') }}
        GROUP BY store_id, sales_date
    ) before
      ON before.store_id = today.store_id
     AND before.sales_date IN (today.sales_date - 7, today.sales_date - 14,
                               today.sales_date - 21, today.sales_date - 28)
    WHERE NOT EXISTS (SELECT 1 FROM {{ source('clean', 'calendar') }} c WHERE c.store_id = today.store_id
                      AND c.calendar_date IN (today.sales_date, before.sales_date))
    GROUP BY today.store_id, today.sales_date, today.lines
) n
        WHERE days_compared >= 3
          AND (lines < 0.6 * normal_lines OR lines > 1.4 * normal_lines)
