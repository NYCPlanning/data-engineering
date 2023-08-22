DELETE FROM qaqc_aggregate
WHERE
    v =: 'VERSION'
    AND condo::boolean =: CONDO
    AND mapped::boolean =: MAPPED;

INSERT INTO qaqc_aggregate (
    SELECT
        sum(unitsres::numeric)::bigint AS unitsres,
        sum(lotarea::numeric)::bigint AS lotarea,
        sum(bldgarea::numeric)::bigint AS bldgarea,
        sum(comarea::numeric)::bigint AS comarea,
        sum(resarea::numeric)::bigint AS resarea,
        sum(officearea::numeric)::bigint AS officearea,
        sum(retailarea::numeric)::bigint AS retailarea,
        sum(garagearea::numeric)::bigint AS garagearea,
        sum(strgearea::numeric)::bigint AS strgearea,
        sum(factryarea::numeric)::bigint AS factryarea,
        sum(otherarea::numeric)::bigint AS otherarea,
        sum(assessland::numeric)::bigint AS assessland,
        sum(assesstot::numeric)::bigint AS assesstot,
        sum(exempttot::numeric)::bigint AS exempttot,
        sum(firm07_flag::numeric)::bigint AS firm07_flag,
        sum(pfirm15_flag::numeric)::bigint AS pfirm15_flag,
        : 'VERSION' AS v,
        : CONDO AS condo,
        : MAPPED AS mapped
    FROM archive_pluto
:CONDITION
);
