-- G/N/X CommonPlaces whose B7SC is shorter than 8 characters (blank included). Prod logs
-- these as "[DATA ERROR] The B7SC value of '' is not valid" and leaves them out of SAF;
-- so do we (int__saf_segments keeps no CommonPlace without a B7SC, and a short one finds
-- no place name in int__saf_gnx).
SELECT
    'error' AS log_level,
    'saf record with invalid b7sc' AS error_category,
    globalid,
    'commonplace' AS source_feature_layer,
    'placeid' AS record_id_type,
    placeid AS record_id,
    FORMAT(
        '[DATA ERROR] The B7SC value of ''{}'' is not valid.  The record will not be included in the output file.',
        COALESCE(b7sc, '')
    ) AS message
FROM {{ source('recipe_sources', 'dcp_cscl_commonplace_gdb') }}
WHERE
    saftype IN ('G', 'N', 'X')
    AND LENGTH(TRIM(COALESCE(b7sc, ''))) < 8
