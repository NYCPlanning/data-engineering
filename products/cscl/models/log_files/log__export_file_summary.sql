-- One row per exported flat file, like prod's "Total <file> output records: N" lines.
-- Files prod writes such a line for carry that line's log_messages id; the rest share
-- CSCL-LOG-OURS-05. The (export model, filename, id) triples mirror the flat_files group
-- of recipe.yml's exports; keep them in step when an export is added or renamed. log.csv
-- and its attachments (log_face_code_missing.csv, log_altnames_sname_cut.csv,
-- log_name_table_warnings.csv) are left out.
{%- set export_files = [
    ('manhattan_lion_dat', 'ManhattanLION.dat', 'CSCL-LOG-OURS-05'),
    ('bronx_lion_dat', 'BronxLION.dat', 'CSCL-LOG-OURS-05'),
    ('brooklyn_lion_dat', 'BrooklynLION.dat', 'CSCL-LOG-OURS-05'),
    ('queens_lion_dat', 'QueensLION.dat', 'CSCL-LOG-OURS-05'),
    ('staten_island_lion_dat', 'StatenIslandLION.dat', 'CSCL-LOG-OURS-05'),
    ('ldf_base', 'LDFBASE.dat', 'CSCL-LOG-OURS-05'),
    ('ldf_header', 'LDFHEADER.dat', 'CSCL-LOG-OURS-05'),
    ('saf_abcegnpx_generic', 'GenericABCEGNPX.txt', 'CSCL-LOG-SAF-05'),
    ('saf_abcegnpx_roadbed', 'RoadbedABCEGNPX.txt', 'CSCL-LOG-SAF-05'),
    ('saf_d_generic', 'GenericD.txt', 'CSCL-LOG-SAF-03'),
    ('saf_d_roadbed', 'RoadbedD.txt', 'CSCL-LOG-SAF-03'),
    ('saf_i', 'Intersection.txt', 'CSCL-LOG-SAF-01'),
    ('saf_ov_generic', 'GenericOV.txt', 'CSCL-LOG-SAF-04'),
    ('saf_ov_roadbed', 'RoadbedOV.txt', 'CSCL-LOG-SAF-04'),
    ('saf_s_generic', 'GenericS.txt', 'CSCL-LOG-SAF-02'),
    ('saf_s_roadbed', 'RoadbedS.txt', 'CSCL-LOG-SAF-02'),
    ('sedat', 'SEDAT.txt', 'CSCL-LOG-OURS-05'),
    ('special_sedat', 'SpecialSEDAT.txt', 'CSCL-LOG-OURS-05'),
    ('snd', 'SND.txt', 'CSCL-LOG-SND-03'),
    ('exception', 'Exception.txt', 'CSCL-LOG-EXC-05'),
    ('universal_word', 'Universal.txt', 'CSCL-LOG-WT-01'),
    ('last_word', 'LastWord.txt', 'CSCL-LOG-WT-01'),
    ('enders', 'Enders.txt', 'CSCL-LOG-END-03'),
    ('manhattan_face_code', 'ManhattanFace.txt', 'CSCL-LOG-FC-05'),
    ('bronx_face_code', 'BronxFace.txt', 'CSCL-LOG-FC-05'),
    ('brooklyn_face_code', 'BrooklynFace.txt', 'CSCL-LOG-FC-05'),
    ('queens_face_code', 'QueensFace.txt', 'CSCL-LOG-FC-05'),
    ('staten_island_face_code', 'StatenIslandFace.txt', 'CSCL-LOG-FC-05'),
    ('rpl', 'RPL.txt', 'CSCL-LOG-RPL-09'),
    ('thinlion_all', 'nyc.thinlion', 'CSCL-LOG-OURS-05'),
    ('thinlion_bronx', 'BronxThinLION.txt', 'CSCL-LOG-OURS-05'),
    ('thinlion_brooklyn', 'BrooklynThinLION.txt', 'CSCL-LOG-OURS-05'),
    ('thinlion_manhattan', 'ManhattanThinLION.txt', 'CSCL-LOG-OURS-05'),
    ('thinlion_queens', 'QueensThinLION.txt', 'CSCL-LOG-OURS-05'),
    ('thinlion_statenisland', 'StatenIslandThinLION.txt', 'CSCL-LOG-OURS-05'),
    ('thinfire_bronx', 'BronxThinFire.txt', 'CSCL-LOG-OURS-05'),
    ('thinfire_brooklyn', 'BrooklynThinFire.txt', 'CSCL-LOG-OURS-05'),
    ('thinfire_manhattan', 'ManhattanThinFire.txt', 'CSCL-LOG-OURS-05'),
    ('thinfire_queens', 'QueensThinFire.txt', 'CSCL-LOG-OURS-05'),
    ('thinfire_statenisland', 'StatenIslandThinFire.txt', 'CSCL-LOG-OURS-05'),
    ('thined_dat', 'thined.txt', 'CSCL-LOG-OURS-05'),
    ('cdta2020_dat', 'CDTA2020.txt', 'CSCL-LOG-OURS-05'),
    ('cdta2020_csv', 'CDTA2020.csv', 'CSCL-LOG-OURS-05'),
    ('nta2020_dat', 'NTA2020.txt', 'CSCL-LOG-OURS-05'),
    ('nta2020_csv', 'NTA2020.csv', 'CSCL-LOG-OURS-05'),
    ('nta2010_csv', 'NTA.csv', 'CSCL-LOG-OURS-05'),
    ('nta2010_txt', 'NTA.txt', 'CSCL-LOG-OURS-05')
] %}

{% for model, filename, log_message_id in export_files %}
    SELECT
        'info' AS log_level,
        'output file summary' AS error_category,
        '{{ log_message_id }}' AS log_message_id,
        '{{ filename }}' AS output_filename,
        COUNT(*) AS record_count,
        FORMAT('Total {} output records: {}', '{{ filename }}', COUNT(*)) AS message
    FROM {{ ref(model) }}
    {{ "UNION ALL" if not loop.last else "" }}
{% endfor %}
