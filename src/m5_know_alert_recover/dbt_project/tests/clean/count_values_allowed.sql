-- A count is a believable count
--
-- Prepare first: Join each count to its product for the unit: pieces or kilograms
-- The limit:     Not empty, not below zero. Pieces are whole numbers. Counted on its own day, by scanner or by hand
-- When it fails: QUARANTINE: the rows this test returns are set aside with this reason. The good rows carry on.
{{ config(tags=['clean'], severity='warn', meta={'number': 28, 'action': 'quarantine', 'lens': 'data', 'question': 'Valid?', 'protects': 'The replenishment team: they order from this number', 'sets_aside': 'stock_counts'}) }}

SELECT c.store_id, c.count_date AS sales_date, c.product_id, c.counted_qty,
       c.counted_by, c.counted_at
FROM {{ source('clean', 'stock_counts') }} c
LEFT JOIN {{ source('clean', 'products') }} p
  ON p.product_id = c.product_id
 AND c.count_date BETWEEN p.valid_from AND coalesce(p.valid_to, DATE '9999-12-31')
WHERE c.counted_qty IS NULL OR c.counted_qty < 0
   OR (p.unit = 'each' AND c.counted_qty <> round(c.counted_qty))
   OR c.counted_by IS NULL OR c.counted_by NOT IN ('scanner', 'hand')
   OR c.counted_at IS NULL OR CAST(c.counted_at AS DATE) <> c.count_date
