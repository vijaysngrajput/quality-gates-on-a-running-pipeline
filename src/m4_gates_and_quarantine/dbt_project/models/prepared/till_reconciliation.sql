-- Two counts of the same money, side by side: one row per till per day.
--
--   till_cents    what the till counted itself
--   lines_cents   what the sales lines of that till add up to
--
-- A till with no lines counts as zero, so a missing sales file shows as a difference.
SELECT t.store_id,
       t.till_id,
       t.business_date AS sales_date,
       t.net_total_cents AS till_cents,
       coalesce(l.lines_cents, 0) AS lines_cents,
       coalesce(l.lines_cents, 0) - t.net_total_cents AS difference_cents
FROM {{ source('clean', 'till_totals') }} t
LEFT JOIN (
    SELECT store_id, till_id, sales_date, sum(amount_cents) AS lines_cents
    FROM {{ source('clean', 'sales_lines') }}
    GROUP BY store_id, till_id, sales_date
) l
  ON l.store_id = t.store_id AND l.till_id = t.till_id AND l.sales_date = t.business_date
