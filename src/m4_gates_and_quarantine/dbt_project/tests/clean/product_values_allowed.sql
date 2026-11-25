-- A product row makes sense
--
-- Prepare first: Nothing
-- The limit:     A name. A price above zero. A unit of each or kg. A status of active or retired. A start that is not after its end
-- When it fails: BLOCK: the tasks after this gate do not run. The outputs keep their last good numbers.
{{ config(tags=['clean'], severity='error', meta={'number': 19, 'action': 'block', 'lens': 'data', 'question': 'Valid?', 'protects': 'Everyone who reads the two outputs: a sales line is joined to this table'}) }}

SELECT product_id AS thing, valid_from AS day, product_name, unit, unit_price_cents,
       status, valid_to
FROM {{ source('clean', 'products') }}
WHERE product_name IS NULL OR unit_price_cents IS NULL OR unit_price_cents <= 0
   OR unit IS NULL OR unit NOT IN ('each', 'kg')
   OR status IS NULL OR status NOT IN ('active', 'retired')
   OR valid_from IS NULL OR valid_from > valid_to
