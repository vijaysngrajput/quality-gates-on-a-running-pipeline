-- One person is one member
--
-- Prepare first: Clean the email: remove spaces around it, and make it lower case. Then number the members that share it, oldest first
-- The limit:     Each cleaned email belongs to one member_id
-- When it fails: QUARANTINE: the newer member is set aside. The first one carries on, so the person gets one email.
--
-- This test returns member ids only. Never a name, an email or a phone number.
{{ config(tags=['clean'], severity='warn', meta={'number': 34, 'action': 'quarantine', 'lens': 'data', 'question': 'Unique?', 'protects': 'Marketing, and the member who gets the email: two emails, and two sets of points', 'sets_aside': 'members'}) }}

SELECT member_id AS thing, joined_on AS day, first_member_id
FROM (
    SELECT member_id, joined_on,
           row_number() OVER (PARTITION BY lower(trim(email)) ORDER BY joined_on, member_id) AS copy_number,
           min(member_id) OVER (PARTITION BY lower(trim(email))) AS first_member_id
    FROM {{ source('clean', 'members') }}
) people
WHERE copy_number > 1
