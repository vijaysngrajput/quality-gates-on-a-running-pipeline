-- An email has the shape of an email
--
-- Prepare first: Take the email apart at the @ sign
-- The limit:     One @, with text before it. A dot after it. No space inside
-- When it fails: QUARANTINE: the rows this test returns are set aside with this reason. The good rows carry on.
{{ config(tags=['clean'], severity='warn', meta={'number': 35, 'action': 'quarantine', 'lens': 'data', 'question': 'Valid?', 'protects': 'Marketing, and the member who gets the email. An email that cannot be delivered, or reaches a stranger', 'sets_aside': 'members'}) }}

SELECT member_id AS thing, joined_on AS day, 'the email has the wrong shape' AS problem
FROM {{ source('clean', 'members') }}
WHERE email IS NULL
   OR length(email) - length(replace(email, '@', '')) <> 1
   OR strpos(email, '@') < 2
   OR strpos(substr(email, strpos(email, '@') + 2), '.') = 0
   OR strpos(trim(email), ' ') > 0
   OR substr(email, length(email), 1) = '.'
