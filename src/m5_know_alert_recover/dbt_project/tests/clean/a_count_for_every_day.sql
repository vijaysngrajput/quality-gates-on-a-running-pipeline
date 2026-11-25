-- A snapshot has a row for every day
--
-- Prepare first: Make the list of counts a day should have: every open store, every product on sale that day. Then look for the ones that are not there
-- The limit:     Every product on sale has a count, in every open store, on every day
-- When it fails: WARN: the run continues, and the warning is recorded.
{{ config(tags=['clean'], severity='warn', meta={'number': 26, 'action': 'warn', 'lens': 'data', 'question': 'Complete?', 'protects': 'The replenishment team: they order from this number. A missing count is read as zero, and the store over-orders'}) }}

SELECT e.store_id, e.day AS sales_date, p.product_id, p.product_name,
               'no count for this product on this day' AS problem
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
        JOIN {{ source('clean', 'products') }} p
          ON e.day BETWEEN p.valid_from AND coalesce(p.valid_to, DATE '9999-12-31')
        LEFT JOIN {{ source('clean', 'stock_counts') }} c
          ON c.store_id = e.store_id AND c.product_id = p.product_id AND c.count_date = e.day
        WHERE c.product_id IS NULL
