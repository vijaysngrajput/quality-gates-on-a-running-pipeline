-- A supplier row fits its product
--
-- Prepare first: Group by the key. Join each row to the product for its unit
-- The limit:     Each supplier + product + date is there once. The product exists. The unit is the product's unit
-- When it fails: QUARANTINE: the rows this test returns are set aside with this reason. The good rows carry on.
{{ config(tags=['clean'], severity='warn', meta={'number': 33, 'action': 'quarantine', 'lens': 'data', 'question': 'Consistent?', 'protects': 'Finance: they pay suppliers from this price. A cost per kilogram read as a cost per piece is a wrong payment', 'sets_aside': 'supplier_prices'}) }}

SELECT s.supplier_id || ' ' || s.product_id AS thing, s.price_date AS day,
       s.supplier_id, s.product_id, s.unit AS supplier_unit, p.unit AS product_unit,
       'the product is unknown, or its unit differs' AS problem
FROM {{ source('clean', 'supplier_prices') }} s
LEFT JOIN {{ source('clean', 'products') }} p
  ON p.product_id = s.product_id
 AND s.price_date BETWEEN p.valid_from AND coalesce(p.valid_to, DATE '9999-12-31')
WHERE p.product_id IS NULL OR p.unit <> s.unit
UNION ALL
SELECT supplier_id || ' ' || product_id AS thing, price_date AS day,
       supplier_id, product_id, NULL AS supplier_unit, NULL AS product_unit,
       'the same row is there more than once' AS problem
FROM {{ source('clean', 'supplier_prices') }}
GROUP BY supplier_id, product_id, price_date
HAVING count(*) > 1
