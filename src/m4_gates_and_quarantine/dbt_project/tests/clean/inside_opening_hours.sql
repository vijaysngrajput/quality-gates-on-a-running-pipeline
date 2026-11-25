-- Nothing is sold while the store is closed
--
-- Prepare first: Join each line to its store for the opening hours, and to the calendar for a day with other hours
-- The limit:     sold_at is between opens_at and closes_at of that store on that day
-- When it fails: QUARANTINE: the rows this test returns are set aside with this reason. The good rows carry on.
{{ config(tags=['clean'], severity='warn', meta={'number': 9, 'action': 'quarantine', 'lens': 'business', 'question': 'Valid?', 'protects': 'Store managers and the head of retail operations: net sales, so pay. A wrong time can be a wrong day, and a wrong week', 'sets_aside': 'sales_lines'}) }}

SELECT l.line_id, l.store_id, l.sales_date, l.sold_at,
       coalesce(c.opens_at, s.opens_at) AS opens_at,
       coalesce(c.closes_at, s.closes_at) AS closes_at, l.amount_cents
FROM {{ source('clean', 'sales_lines') }} l
JOIN {{ source('clean', 'stores') }} s ON s.store_id = l.store_id
LEFT JOIN {{ source('clean', 'calendar') }} c
  ON c.store_id = l.store_id AND c.calendar_date = l.sales_date
WHERE CAST(l.sold_at AS TIME) < coalesce(c.opens_at, s.opens_at)
   OR CAST(l.sold_at AS TIME) > coalesce(c.closes_at, s.closes_at)
