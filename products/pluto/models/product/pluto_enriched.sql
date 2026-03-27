{{
    config(
        materialized='table',
        indexes=[{'columns': ['bbl'], 'unique': True}],
        tags=['pluto_enrichment']
    )
}}

-- Assemble enriched PLUTO data from intermediate models
-- This model joins all simple enrichment models to the base pluto table
-- The results are used to batch UPDATE the pluto table

SELECT
    p.bbl,
    far.builtfar,
    far.residfar,
    far.commfar,
    far.facilfar,
    far.affresfar,
    far.mnffar,
    irr.irrlotcode,
    san.sanitboro,
    san.sanitdistrict,
    lu.landuse,
    lu.areasource,
    ot.ownertype,
    ed.edesignum,
    -- ll.latitude,
    -- ll.longitude,
    -- ll.centroid,
    cn.condono,
    lpc.histdist,
    lpc.landmark,
    nf.lotdepth,
    nf.numfloors,
    nf.lotarea,
    nf.sanborn,
    mih.mih_opt1,
    mih.mih_opt2,
    mih.mih_opt3,
    mih.mih_opt4,
    tz.trnstzone,
    bsmt.bsmtcode,
    lot.lottype,
    prox.proxcode,
    ease.easements
FROM {{ target.schema }}.pluto AS p
LEFT JOIN {{ ref('int_pluto__far') }} AS far ON p.bbl = far.bbl
LEFT JOIN {{ ref('int_pluto__irrlotcode') }} AS irr ON p.bbl = irr.bbl
LEFT JOIN {{ ref('int_pluto__sanitation') }} AS san ON p.bbl = san.bbl
LEFT JOIN {{ ref('int_pluto__landuse') }} AS lu ON p.bbl = lu.bbl
LEFT JOIN {{ ref('int_pluto__ownertype') }} AS ot ON p.bbl = ot.bbl
LEFT JOIN {{ ref('int_pluto__edesignation') }} AS ed ON p.bbl = ed.bbl
-- LEFT JOIN {{ ref('int_pluto__latlong') }} AS ll ON p.bbl = ll.bbl
LEFT JOIN {{ ref('int_pluto__condono') }} AS cn ON p.bbl = cn.bbl
LEFT JOIN {{ ref('int_pluto__lpc') }} AS lpc ON p.bbl = lpc.bbl
LEFT JOIN {{ ref('int_pluto__numericfields') }} AS nf ON p.bbl = nf.bbl
LEFT JOIN {{ ref('int_pluto__miharea') }} AS mih ON p.bbl = mih.bbl
LEFT JOIN {{ ref('int_pluto__transitzone') }} AS tz ON p.bbl = tz.bbl
LEFT JOIN {{ ref('int_pluto__bsmtcode') }} AS bsmt ON p.bbl = bsmt.bbl
LEFT JOIN {{ ref('int_pluto__lottype') }} AS lot ON p.bbl = lot.bbl
LEFT JOIN {{ ref('int_pluto__proxcode') }} AS prox ON p.bbl = prox.bbl
LEFT JOIN {{ ref('int_pluto__easements') }} AS ease ON p.bbl = ease.bbl
