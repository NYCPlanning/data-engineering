-- LION gdb SAF replicants (int__saf_replicant_fields, on a LION gdb segment) with the B7SC
-- prod names each by (ExtractorClass.cs SetSAFStreetNameAndCode, :4970-5215): B5SC + LGC1
-- for AltSegmentData B-F/O/P, B7SC_VANITY for AddressPoint V and B7SC_ACTUAL for S, and
-- the CommonPlace B7SC for G/N/X. Type A takes its segment's name and isn't looked up, so
-- it is left out.
WITH lion AS (
    SELECT lionkey FROM {{ ref('int__lion') }}
    WHERE include_in_bytes_lion
),
replicants AS (
    SELECT DISTINCT
        replicants.saf_globalid,
        replicants.saf_source_table,
        replicants.specaddr,
        replicants.lionkey
    FROM {{ ref('int__saf_replicant_fields') }} AS replicants
    INNER JOIN lion ON replicants.lionkey = lion.lionkey
    WHERE replicants.specaddr <> 'A'
)
SELECT
    replicants.*,
    CASE replicants.saf_source_table
        WHEN 'altsegmentdata' THEN COALESCE(alt.b5sc, '') || COALESCE(alt.lgc1, '')
        WHEN 'commonplace' THEN COALESCE(place.b7sc, '')
        ELSE COALESCE(CASE replicants.specaddr WHEN 'V' THEN ap.b7sc_vanity ELSE ap.b7sc_actual END, '')
    END AS b7sc
FROM replicants
LEFT JOIN {{ source('recipe_sources', 'dcp_cscl_altsegmentdata') }} AS alt
    ON replicants.saf_globalid = alt.globalid
LEFT JOIN {{ source('recipe_sources', 'dcp_cscl_commonplace_gdb') }} AS place
    ON replicants.saf_globalid = place.globalid
LEFT JOIN {{ source('recipe_sources', 'dcp_cscl_addresspoints') }} AS ap
    ON replicants.saf_globalid = ap.globalid
