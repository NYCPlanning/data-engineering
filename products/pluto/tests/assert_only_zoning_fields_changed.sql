{{ 
    config(
        tags = ['de_check', 'minor'], 
        severity = 'warn',
        meta = {
            'description': '''
				This test checks for changes in fields other than zoning between the current and previous PLUTO versions. 
                This test is expected to pass for PLUTO Minor versions only.
			''',
            'next_steps': '''
				Investigate if correct input datasets were used, and what went wrong in the `plan` step.
                If build was `plan`ned
                This needs to be addressed prior to promoting PLUTO for GIS review.
			'''
        }
    ) 
}}

{% set columns = [
    "borough",
    "block",
    "lot",
    "cd",
    "ct2010",
    "cb2010",
    "schooldist",
    "council",
    "zipcode",
    "firecomp",
    "policeprct",
    "healtharea",
    "sanitboro",
    "sanitsub",
    "address",
    "easements",
    "ownertype",
    "ownername",
    "lotarea",
    "bldgarea",
    "comarea",
    "resarea",
    "officearea",
    "retailarea",
    "garagearea",
    "strgearea",
    "factryarea",
    "otherarea",
    "areasource",
    "numbldgs",
    "numfloors",
    "unitsres",
    "unitstotal",
    "lotfront",
    "lotdepth",
    "bldgfront",
    "bldgdepth",
    "ext",
    "proxcode",
    "irrlotcode",
    "lottype",
    "bsmtcode",
    "assessland",
    "assesstot",
    "exempttot",
    "yearbuilt",
    "yearalter1",
    "yearalter2",
    "histdist",
    "landmark",
    "builtfar",
    "borocode",
    "condono",
    "tract2010",
    "xcoord",
    "ycoord",
    "latitude",
    "longitude",
    "sanborn",
    "appbbl",
    "appdate",
    "plutomapid",
    "sanitdistrict",
    "healthcenterdistrict",
    "firm07_flag",
    "pfirm15_flag"
] %}

-- TODO - this type nightmare should be resolved in repo/ingest
-- a good chunk of this will be taken care of simply moving to ingest to archive pluto in recipes
-- though some of the types in export_pluto I don't agree with
{% set numeric_columns = [
    "block",
    "lot",
    "cd",
    "council",
    "zipcode",
    "policeprct",
    "healthcenterdistrict",
    "healtharea",
    "easements",
    "lotarea",
    "bldgarea",
    "comarea",
    "resarea",
    "officearea",
    "retailarea",
    "garagearea",
    "strgearea",
    "factryarea",
    "otherarea",
    "numbldgs",
    "numfloors",
    "unitsres",
    "unitstotal",
    "lotfront",
    "lotdepth",
    "bldgfront",
    "bldgdepth",
    "assessland",
    "assesstot",
    "exempttot",
    "yearbuilt",
    "yearalter1",
    "yearalter2",
    "builtfar",
    "borocode",
    "condono",
    "xcoord",
    "ycoord",
    "appbbl"
] %}

WITH current AS (
    SELECT
        {{ dbt_utils.star(
            from=source('build_sources', 'export_pluto'), except=["latitude", "longitude"]
        ) }},
        ROUND(latitude, 6) AS latitude,
        ROUND(longitude, 6) AS longitude
    FROM {{ source('build_sources', 'export_pluto') }}
),

prev AS (
    SELECT
        bbl::decimal::bigint,
        {% for col in columns %}
            {%- if col in numeric_columns -%}
                
                {{ col }}::numeric,
            {%- elif col not in ["latitude", "longitude"] -%}
                
                {{ col }},
            {%- endif -%}
        {% endfor %}
        ROUND(latitude::decimal, 6) AS latitude,
        ROUND(longitude::decimal, 6) AS longitude
    FROM {{ source('build_sources', 'previous_pluto') }}
),

mismatches AS (
    SELECT
        current.bbl,
        JSONB_BUILD_ARRAY(
            {% for col in columns %}
                JSONB_BUILD_OBJECT(
                    'column', '{{ col }}',
                    'previous', prev.{{ col }},
                    'current', current.{{ col }}
                )
                {%- if not loop.last -%},{% endif %}
            {%- endfor -%}
        ) AS mismatches
    FROM current
    INNER JOIN prev
        ON current.bbl = prev.bbl
    WHERE
        {% for col in columns -%}
            current.{{ col }} IS DISTINCT FROM prev.{{ col }}
            {%- if not loop.last %}
                OR
            {% endif -%}
        {%- endfor %}
),

unnested AS (
    SELECT
        bbl,
        JSONB_ARRAY_ELEMENTS(mismatches) AS mismatch
    FROM mismatches
)

SELECT
    bbl,
    j.*
FROM unnested,
    LATERAL JSONB_TO_RECORD(mismatch) AS j ("column" text, "previous" text, "current" text)
WHERE current IS DISTINCT FROM previous
