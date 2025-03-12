CREATE INDEX acs_current_latest_draft_idx ON ar_build_auto.acs_current_latest_draft (labs_geoid, pff_variable);
CREATE INDEX acs_previous_latest_draft_idx ON ar_build_auto.acs_previous_latest_draft (labs_geoid, pff_variable);

SELECT census_geoid, labs_geoid, geotype, labs_geotype, pff_variable, c, e, m, p, z, "domain", ogc_fid, data_library_version
FROM ar_build_auto.acs_current_latest_draft;


-- ACS Current
with draft as (
SELECT labs_geoid, labs_geotype, sum(e::float), count(e)
FROM ar_build_auto.acs_current_latest_draft
group by labs_geoid, labs_geotype
),
pub as (
SELECT labs_geoid, labs_geotype, sum(e::float), count(e)
FROM ar_build_auto.acs_current_latest_published
group by labs_geoid, labs_geotype
)
select * from draft join pub on draft.labs_geoid = pub.labs_geoid and draft.labs_geotype = pub.labs_geotype
where draft.count = pub.count;


-- ACS Previous
with prev as (
SELECT labs_geoid, labs_geotype, sum(e::float), count(e)
FROM ar_build_auto.acs_previous_latest_draft
group by labs_geoid, labs_geotype
),
curr as (
SELECT labs_geoid, labs_geotype, sum(e::float), count(e)
FROM ar_build_auto.acs_current_latest_draft
group by labs_geoid, labs_geotype
)
select * from prev join curr on prev.labs_geoid = curr.labs_geoid and prev.labs_geotype = curr.labs_geotype
where prev.count <> curr.count;



-- Exists in published, Null in Draft
SELECT
    d.labs_geoid,
    d.labs_geotype,
    d.pff_variable,
    p.c as published_c,
    p.e as published_e,
    p.m as published_m,
    p.p as published_p,
    p.z as published_z,
    d.c as draft_c,
    d.e as draft_e,
    d.m as draft_m,
    d.p as draft_p,
    d.z as draft_z
into ar_build_auto.qa_null_acs_draft_nonnull_pub
FROM acs_current_latest_draft d
JOIN acs_current_latest_published p
    ON d.labs_geoid = p.labs_geoid
    AND d.labs_geotype = p.labs_geotype
    AND d.pff_variable = p.pff_variable
WHERE  (d.c IS NULL AND p.c IS NOT NULL)
    OR (d.e IS NULL AND p.e IS NOT NULL)
    OR (d.m IS NULL AND p.m IS NOT NULL)
    OR (d.p IS NULL AND p.p IS NOT NULL)
    OR (d.z IS NULL AND p.z IS NOT NULL)
ORDER BY d.labs_geoid, d.pff_variable;


-- Which district had the most flips from non-null to null?
select labs_geoid, labs_geotype, count(*) as cnt from ar_build_auto.qa_null_acs_draft_nonnull_pub
where CAST(published_c AS float) < 20.0 and labs_geotype not like '%City%' and labs_geotype not like '%Boro%'
group by labs_geoid, labs_geotype order by cnt desc
