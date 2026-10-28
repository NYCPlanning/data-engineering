-- A reviewer status past not_reviewed (presented, agreed, disputed) needs the date it
-- changed and where (reviewed_on, review_ref); a file sign-off of approved or rejected
-- needs signoff_date and signoff_ref. Reads only seeds.

SELECT
    'known_differences' AS seed,
    id,
    'reviewer status recorded without reviewed_on and review_ref' AS problem
FROM {{ ref('known_differences') }}
WHERE
    (
        gr_status IN ('presented', 'agreed', 'disputed')
        OR gss_status IN ('presented', 'agreed', 'disputed')
        OR gis_status IN ('presented', 'agreed', 'disputed')
    )
    AND (reviewed_on IS NULL OR coalesce(trim(review_ref), '') = '')

UNION ALL

SELECT
    'lion_outputs' AS seed,
    file_id AS id,
    'sign-off recorded without signoff_date and signoff_ref' AS problem
FROM {{ ref('lion_outputs') }}
WHERE
    (
        gr_signoff IN ('approved', 'rejected')
        OR gss_signoff IN ('approved', 'rejected')
        OR gis_signoff IN ('approved', 'rejected')
    )
    AND (signoff_date IS NULL OR coalesce(trim(signoff_ref), '') = '')
