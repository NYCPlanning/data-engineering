-- One row per exported flat file, like prod's "Total <file> output records: N" lines.
-- (export model, filename) pairs mirror the flat_files group of recipe.yml's exports; keep
-- them in step when an export is added or renamed. log.csv itself is left out.
{%- set export_files = [
    ('manhattan_lion_dat', 'ManhattanLION.dat'),
    ('bronx_lion_dat', 'BronxLION.dat'),
    ('brooklyn_lion_dat', 'BrooklynLION.dat'),
    ('queens_lion_dat', 'QueensLION.dat'),
    ('staten_island_lion_dat', 'StatenIslandLION.dat'),
    ('ldf_base', 'LDFBASE.dat'),
    ('ldf_header', 'LDFHEADER.dat'),
    ('saf_abcegnpx_generic', 'GenericABCEGNPX.txt'),
    ('saf_abcegnpx_roadbed', 'RoadbedABCEGNPX.txt'),
    ('saf_d_generic', 'GenericD.txt'),
    ('saf_d_roadbed', 'RoadbedD.txt'),
    ('saf_i', 'Intersection.txt'),
    ('saf_ov_generic', 'GenericOV.txt'),
    ('saf_ov_roadbed', 'RoadbedOV.txt'),
    ('saf_s_generic', 'GenericS.txt'),
    ('saf_s_roadbed', 'RoadbedS.txt'),
    ('sedat', 'SEDAT.txt'),
    ('special_sedat', 'SpecialSEDAT.txt'),
    ('snd', 'SND.txt'),
    ('exception', 'Exception.txt'),
    ('universal_word', 'Universal.txt'),
    ('last_word', 'LastWord.txt'),
    ('enders', 'Enders.txt'),
    ('manhattan_face_code', 'ManhattanFace.txt'),
    ('bronx_face_code', 'BronxFace.txt'),
    ('brooklyn_face_code', 'BrooklynFace.txt'),
    ('queens_face_code', 'QueensFace.txt'),
    ('staten_island_face_code', 'StatenIslandFace.txt'),
    ('rpl', 'RPL.txt'),
    ('thinlion_all', 'nyc.thinlion'),
    ('thinlion_bronx', 'BronxThinLION.txt'),
    ('thinlion_brooklyn', 'BrooklynThinLION.txt'),
    ('thinlion_manhattan', 'ManhattanThinLION.txt'),
    ('thinlion_queens', 'QueensThinLION.txt'),
    ('thinlion_statenisland', 'StatenIslandThinLION.txt'),
    ('thinfire_bronx', 'BronxThinFire.txt'),
    ('thinfire_brooklyn', 'BrooklynThinFire.txt'),
    ('thinfire_manhattan', 'ManhattanThinFire.txt'),
    ('thinfire_queens', 'QueensThinFire.txt'),
    ('thinfire_statenisland', 'StatenIslandThinFire.txt'),
    ('thined_dat', 'thined.txt'),
    ('cdta2020_dat', 'CDTA2020.txt'),
    ('cdta2020_csv', 'CDTA2020.csv'),
    ('nta2020_dat', 'NTA2020.txt'),
    ('nta2020_csv', 'NTA2020.csv'),
    ('nta2010_csv', 'NTA.csv'),
    ('nta2010_txt', 'NTA.txt')
] %}

{% for model, filename in export_files %}
    SELECT
        'info' AS log_level,
        'output file summary' AS error_category,
        '{{ filename }}' AS output_filename,
        COUNT(*) AS record_count,
        FORMAT('Total {} output records: {}', '{{ filename }}', COUNT(*)) AS message
    FROM {{ ref(model) }}
    {{ "UNION ALL" if not loop.last else "" }}
{% endfor %}
