-- A member row has what marketing needs
--
-- Prepare first: Decide which cells may be empty: a phone number is optional in the app
-- The limit:     A name, a home store, a join date, and a yes or no to marketing. A phone may be empty
-- When it fails: QUARANTINE: the rows this test returns are set aside with this reason. The good rows carry on.
{{ config(tags=['clean'], severity='warn', meta={'number': 36, 'action': 'quarantine', 'lens': 'data', 'question': 'Complete?', 'protects': 'Marketing, and the member who gets the email. An email to someone who did not say yes is against the rules', 'sets_aside': 'members'}) }}

SELECT member_id AS thing, joined_on AS day,
       'a cell that must be filled is empty' AS problem
FROM {{ source('clean', 'members') }}
WHERE member_id IS NULL OR full_name IS NULL OR trim(full_name) = ''
   OR home_store_id IS NULL OR joined_on IS NULL OR marketing_opt_in IS NULL
