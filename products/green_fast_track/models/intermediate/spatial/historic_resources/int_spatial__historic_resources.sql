WITH historic_resources AS (
{{ dbt_utils.union_relations(
    relations=[
        ref('stg__lpc_landmarks'),
        ref('stg__nysshpo_historic_buildings')
    ],
    source_column_name="source_relation",
    include=["variable_type", "variable_id", "raw_geom"],
    column_override={"raw_geom": "geometry"}
) }}
),
-- Note: without `column_override`, dbt throws an error trying to cast.
-- e.g.: `cast("raw_geom" as USER-DEFINED) as "raw...`

pluto AS (
    SELECT geom
    FROM {{ ref('stg__pluto') }}
),

historic_resources_with_pluto AS (
    SELECT
        h.source_relation,
        h.variable_type,
        h.variable_id,
        h.raw_geom,
        p.geom AS lot_geom
    FROM historic_resources AS h
    LEFT JOIN pluto AS p ON ST_WITHIN(h.raw_geom, p.geom)
),

final AS (
    SELECT
        source_relation,
        'historic_resources' AS flag_id_field_name,
        variable_type,
        variable_id,
        ST_MULTI(raw_geom) AS raw_geom,
        ST_MULTI(lot_geom) AS lot_geom
    FROM historic_resources_with_pluto
)

SELECT * FROM final
