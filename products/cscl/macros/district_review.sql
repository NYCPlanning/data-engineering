{% macro district_review_layers() %}
{#-
  Every district_gdb layer reviewed by models/etl_dev_qa/district_review, keyed as in
  seeds/lion_outputs.csv's key_columns (the same keys compare_gdb.py uses).

  key: build-side expression over gdb_<name>. prod_key: the same over
  production_outputs.fgdb_<name>, whose columns are lowercased; defaults to key
  lowercased with quotes stripped, so only composite keys need it. key_name: the field
  name the key gets in the review FGDB (composite keys joined with '_', their values
  with '|').

  nyura: prod's fgdb_nyura carries a stale copy of nybid's schema (no uraid column,
  Bug 013) and has no rows, so it has no prod key; every built URA is new_only.
-#}
{%- do return([
    {'name': 'nyad', 'key': '"AssemDist"'},
    {'name': 'nyadwi', 'key': '"AssemDist"'},
    {'name': 'nyap', 'key': '"ATOMICID"'},
    {'name': 'nybb', 'key': '"BoroCode"'},
    {'name': 'nybbwi', 'key': '"BoroCode"'},
    {'name': 'nybid', 'key': '"BIDID"'},
    {'name': 'nycb2010', 'key': '"BCTCB2010"'},
    {'name': 'nycb2010wi', 'key': '"BCTCB2010"'},
    {'name': 'nycb2020', 'key': '"BCTCB2020"'},
    {'name': 'nycb2020wi', 'key': '"BCTCB2020"'},
    {'name': 'nycc', 'key': '"CounDist"'},
    {'name': 'nyccwi', 'key': '"CounDist"'},
    {'name': 'nycd', 'key': '"BoroCD"'},
    {'name': 'nycdta2020', 'key': '"CDTA2020"'},
    {'name': 'nycdwi', 'key': '"BoroCD"'},
    {'name': 'nycg', 'key': '"CongDist"'},
    {'name': 'nycgwi', 'key': '"CongDist"'},
    {'name': 'nyct2010', 'key': '"BoroCT2010"'},
    {'name': 'nyct2010wi', 'key': '"BoroCT2010"'},
    {'name': 'nyct2020', 'key': '"BoroCT2020"'},
    {'name': 'nyct2020wi', 'key': '"BoroCT2020"'},
    {'name': 'nyed', 'key': '"ElectDist"'},
    {'name': 'nyedwi', 'key': '"ElectDist"'},
    {'name': 'nyfb', 'key': '"FireBN"'},
    {'name': 'nyfc', 'key': '("FireCoType"::text || \'|\' || "FireCoNum"::text)',
     'prod_key': "(firecotype::text || '|' || fireconum::text)", 'key_name': 'FireCoType_FireCoNum'},
    {'name': 'nyfd', 'key': '"FireDiv"'},
    {'name': 'nyha', 'key': '("BoroCode"::text || \'|\' || "HealthArea"::text)',
     'prod_key': "(borocode::text || '|' || healtharea::text)", 'key_name': 'BoroCode_HealthArea'},
    {'name': 'nyhc', 'key': '"HCentDist"'},
    {'name': 'nyhd', 'key': '"NAME"'},
    {'name': 'nyhez', 'key': '"HURRICANE_EVACUATION_ZONE"'},
    {'name': 'nymc', 'key': '("BoroCode"::text || \'|\' || "MuniCourt"::text)',
     'prod_key': "(borocode::text || '|' || municourt::text)", 'key_name': 'BoroCode_MuniCourt'},
    {'name': 'nymcea', 'key': '("BOROCODE"::text || \'|\' || "MCEA"::text)',
     'prod_key': "(borocode::text || '|' || mcea::text)", 'key_name': 'BOROCODE_MCEA'},
    {'name': 'nymcwi', 'key': '("BoroCode"::text || \'|\' || "MuniCourt"::text)',
     'prod_key': "(borocode::text || '|' || municourt::text)", 'key_name': 'BoroCode_MuniCourt'},
    {'name': 'nynta2010', 'key': '"NTACode"'},
    {'name': 'nynta2020', 'key': '"NTA2020"'},
    {'name': 'nypp', 'key': '"Precinct"'},
    {'name': 'nypuma2010', 'key': '"PUMA"'},
    {'name': 'nypuma2020', 'key': '"PUMA"'},
    {'name': 'nysd', 'key': '"SchoolDist"'},
    {'name': 'nyss', 'key': '"StSenDist"'},
    {'name': 'nysswi', 'key': '"StSenDist"'},
    {'name': 'nyura', 'key': '"URAID"', 'prod_key': 'NULL'},
    {'name': 'nyzip', 'key': '"ZIP_CODE"'},
]) -%}
{% endmacro %}


{% macro district_review_pair(layer) %}
{#-
  One row per district key of one layer: our geometry (gdb_<name>) and prod's
  (production_outputs.fgdb_<name>, via prod_fgdb_linear), both linear EPSG:2263, full
  outer joined on the key. A key stored as several features (e.g. nysd's SD 10, once
  per borough) is dissolved first, as in geometry_diff_qa, so whole districts are
  compared. is_equal: the two are the same geometry vertex for vertex (normalized WKB),
  the cheap test that lets identical districts skip the overlay.
-#}
{%- set key_name = layer.get('key_name') or (layer.key | replace('"', '')) -%}
{%- set prod_key = layer.get('prod_key') or (layer.key | replace('"', '') | lower) -%}
WITH new AS (
    SELECT
        {{ layer.key }}::text AS district_key,
        CASE
            WHEN count(*) = 1 THEN any_value(linearize(geom))
            ELSE st_union_agg(try(st_makevalid(linearize(geom))))
        END AS geom
    FROM {{ ref('gdb_' ~ layer.name) }}
    GROUP BY 1
),

prior AS (
    SELECT
        {{ prod_key }}::text AS district_key,
        CASE
            WHEN count(*) = 1 THEN any_value(linearize(shape))
            ELSE st_union_agg(try(st_makevalid(linearize(shape))))
        END AS geom
    FROM {{ prod_fgdb_linear('fgdb_' ~ layer.name) }}
    GROUP BY 1
)

SELECT
    '{{ layer.name }}' AS layer,
    '{{ key_name }}' AS key_name,
    coalesce(n.district_key, p.district_key) AS district_key,
    n.geom AS geom_new,
    p.geom AS geom_prior,
    coalesce(st_aswkb(st_normalize(n.geom)) = st_aswkb(st_normalize(p.geom)), FALSE) AS is_equal
FROM new AS n
FULL OUTER JOIN prior AS p ON n.district_key = p.district_key
{% endmacro %}
