{{
    config(
        meta = {
            'description': '''
                Every community district in DCP AHFT tracker (stg__dcp_housing_ahft) should
                also appear among LIFT own BORO/CD values (stg__lift_csv), since
                lift_supplemented joins the two on lift.cd = ahft.borocd (see
                lift_supplemented.sql). A fresh AHFT extract whose CD key derivation breaks, or
                an AHFT CD that genuinely has no LIFT site, would otherwise fail silently as a
                LEFT JOIN non-match rather than surface here.
            ''',
            'next_steps': '''
                1. Confirm the failing community_district/borocd value is a real, current NYC
                   Community District (not a stale code or a transcription error in the AHFT
                   source file).
                2. If it is real, check whether any LIFT bbl should actually resolve to that CD
                   - a borough/CD coding mismatch in dcas_lift own boro/cd columns is also
                   possible, not just the AHFT side.
            '''
        }
    )
}}

SELECT
    ahft.community_district,
    ahft.borocd
FROM {{ ref('stg__dcp_housing_ahft') }} AS ahft
WHERE NOT EXISTS (
    SELECT 1 FROM {{ ref('stg__lift_csv') }} AS lift
    WHERE lift.cd = ahft.borocd
)
