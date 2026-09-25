#!/bin/bash
source ./bash/config.sh
set_error_traps

mkdir -p output

cd output

csv_export pluto_removed_records
csv_export pluto_changes_not_applied
csv_export pluto_changes_applied
zip pluto_changes.zip *
ls | grep -v pluto_changes.zip | xargs rm

cp ../../source_data_versions.csv ./
cp ../../build_metadata.json ./

echo "${VERSION}" > version.txt

echo "Exporting gdbs and shapefiles"

# DEV section start: low disk space

# mappluto.gdb
fgdb_export_pluto mappluto_gdb &

# mappluto_unclipped.gdb
fgdb_export_pluto mappluto_unclipped_gdb &

# mappluto
shp_export_pluto mappluto MULTIPOLYGON &

# mappluto_unclipped
shp_export_pluto mappluto_unclipped MULTIPOLYGON &

# DEV section end: low disk space

wait
echo "Exporting pluto csv"

# Pluto
mkdir -p pluto && (
    cd pluto
    rm -f pluto.zip
    run_sql_command "\COPY ( 
            SELECT * FROM export_pluto
        ) TO STDOUT DELIMITER ',' CSV HEADER;" > pluto.csv
    echo "${VERSION}" > version.txt
    echo "$(wc -l pluto.csv)" >> version.txt
    zip pluto.zip *
    ls | grep -v pluto.zip | xargs rm
)

echo "Exporting DOF"
# Subset of PLUTO for DOF. Named pluto.csv because DOF's scripts read it by
# that name, so it's scoped under dof/ to keep it off the full PLUTO export.
# bbl goes out as a bigint here, unlike the numeric(19,8) the other exports carry.
mkdir -p dof && (
    cd dof
    rm -f pluto.zip
    run_sql_command "\COPY ( 
            SELECT bbl::bigint AS bbl, council, latitude, longitude, schooldist FROM export_pluto
            WHERE bbl is not null
        ) TO STDOUT DELIMITER ',' CSV HEADER;" > pluto.csv
    echo "${VERSION}" > version.txt
    zip pluto.zip *
    ls | grep -v pluto.zip | xargs rm
)

echo "Exporting QAQC"
mkdir -p qaqc && (
    cd qaqc
    for table in qaqc_aggregate qaqc_expected qaqc_mismatch qaqc_null qaqc_outlier qaqc_bbl_diffs
    do
        csv_export $table
    done
)

wait 
cd ..

python3 -m dcpy.connectors.edm.publishing upload -p db-pluto -a public-read
