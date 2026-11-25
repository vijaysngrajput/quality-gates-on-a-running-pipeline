-- A member has a real store and a past join date
--
-- Prepare first: Join each member to stores. Find the newest day the pipeline has loaded
-- The limit:     home_store_id is in the stores table. joined_on is not after the newest loaded day
-- When it fails: WARN: the run continues, and the warning is recorded.
{{ config(tags=['clean'], severity='warn', meta={'number': 37, 'action': 'warn', 'lens': 'business', 'question': 'Consistent?', 'protects': 'Marketing, and the member who gets the email. Offers are sent by home store'}) }}

SELECT m.member_id AS thing, m.joined_on AS day, m.home_store_id,
       'an unknown home store, or a join date in the future' AS problem
FROM {{ source('clean', 'members') }} m
LEFT JOIN {{ source('clean', 'stores') }} s ON s.store_id = m.home_store_id
WHERE s.store_id IS NULL OR m.joined_on > (SELECT CAST(max(_loaded_at) AS DATE) - 1 FROM {{ source('raw', 'sales_lines') }})
