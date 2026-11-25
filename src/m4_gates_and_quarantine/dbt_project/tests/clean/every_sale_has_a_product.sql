-- Every sale is for a product that exists
--
-- Prepare first: Go back to the raw rows, and join each to the product valid on its day
-- The limit:     No raw sales line is without a product or a store
-- When it fails: QUARANTINE: the rows this test returns are set aside with this reason. The good rows carry on.
{{ config(tags=['clean'], severity='warn', meta={'number': 5, 'action': 'quarantine', 'lens': 'data', 'question': 'Consistent?', 'protects': 'Everyone who reads the two outputs: a line with no product is dropped, and no row is left to look at', 'sets_aside': 'raw_sales_lines'}) }}

SELECT l.line_id, l.store_id, CAST(CAST(l.sold_at AS TIMESTAMP) AS DATE) AS sales_date,
       l.product_id, CAST(l.amount_cents AS BIGINT) AS lost_cents
FROM {{ source('raw', 'sales_lines') }} l
LEFT JOIN {{ source('clean', 'products') }} p
  ON p.product_id = l.product_id
 AND CAST(CAST(l.sold_at AS TIMESTAMP) AS DATE)
     BETWEEN p.valid_from AND coalesce(p.valid_to, DATE '9999-12-31')
LEFT JOIN {{ source('clean', 'stores') }} s ON s.store_id = l.store_id
WHERE p.product_id IS NULL OR s.store_id IS NULL
