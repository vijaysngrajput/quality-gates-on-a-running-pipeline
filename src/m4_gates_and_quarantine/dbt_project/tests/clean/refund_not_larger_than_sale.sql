-- A refund is not larger than the sale
--
-- Prepare first: Join each refund to the receipt it names, and add up what that receipt sold of the same product
-- The limit:     The receipt exists, and the money given back is not more than it took
-- When it fails: QUARANTINE: the rows this test returns are set aside with this reason. The good rows carry on.
{{ config(tags=['clean'], severity='warn', meta={'number': 7, 'action': 'quarantine', 'lens': 'business', 'question': 'Consistent?', 'protects': 'Finance: money given back for something that was never sold', 'sets_aside': 'sales_lines'}) }}

SELECT r.line_id, r.store_id, r.sales_date, r.refund_of_receipt_id,
       abs(r.amount_cents) AS given_back_cents, sold.amount_cents AS sold_cents
FROM {{ source('clean', 'sales_lines') }} r
LEFT JOIN (
    SELECT receipt_id, product_id, sum(amount_cents) AS amount_cents
    FROM {{ source('clean', 'sales_lines') }}
    WHERE line_type = 'sale'
    GROUP BY receipt_id, product_id
) sold
  ON sold.receipt_id = r.refund_of_receipt_id AND sold.product_id = r.product_id
WHERE r.line_type = 'refund'
  AND (sold.receipt_id IS NULL OR abs(r.amount_cents) > sold.amount_cents)
