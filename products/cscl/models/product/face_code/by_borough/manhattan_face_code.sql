SELECT *
FROM {{ ref('face_code') }}
WHERE dat_column LIKE '1%'
