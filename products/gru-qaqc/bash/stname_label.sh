#!/bin/bash
set -e
source config.sh

VERSION=$DATE

# Export file
mkdir -p output
mkdir -p output/stname_label && (
    cd output/stname_label
    mc cp spaces/edm-storage/ETL\ Working\ GDB.gdb.zip  ETL.gdb.zip
    psql $BUILD_ENGINE -c "
        CREATE SCHEMA IF NOT EXISTS cscl;
        DROP TABLE IF EXISTS cscl.CENTERLINE;
        DROP TABLE IF EXISTS cscl.SEGMENT_LGC;
        DROP TABLE IF EXISTS cscl.STREETNAME;
    "

    ogr2ogr -progress -f PostgreSQL \
        PG:"host=$BUILD_HOST user=$BUILD_USER port=$BUILD_PORT dbname=$BUILD_DB password=$BUILD_PWD" \
        ETL.gdb.zip -nlt NONE -nln "cscl.centerline" \
        -sql "SELECT segmentid, stname_label FROM centerline" &

    ogr2ogr -progress -f PostgreSQL \
        PG:"host=$BUILD_HOST user=$BUILD_USER port=$BUILD_PORT dbname=$BUILD_DB password=$BUILD_PWD" \
        ETL.gdb.zip SEGMENT_LGC -nlt NONE -nln "cscl.SEGMENT_LGC" \
        -sql "SELECT segmentid, B7SC, PREFERRED_LGC_FLAG FROM SEGMENT_LGC
            WHERE PREFERRED_LGC_FLAG='Y'" &

    ogr2ogr -progress -f PostgreSQL \
        PG:"host=$BUILD_HOST user=$BUILD_USER port=$BUILD_PORT dbname=$BUILD_DB password=$BUILD_PWD" \
        ETL.gdb.zip -nlt NONE -nln "cscl.STREETNAME"\
        -sql "SELECT B7SC, PRINCIPAL_FLAG, PRE_MODIFIER, PRE_DIRECTIONAL, PRE_TYPE, 
            STREET_NAME, POST_TYPE, POST_DIRECTIONAL, POST_MODIFIER FROM STREETNAME
            WHERE PRINCIPAL_FLAG='Y'"
    wait

    psql $BUILD_ENGINE -c "\COPY ( 
        SELECT * 
        FROM (
            SELECT
                a.*,
                CONCAT_WS(' ',
                    trim(b.PRE_MODIFIER), 
                    trim(b.PRE_DIRECTIONAL),
                    trim(b.PRE_TYPE),
                    trim(b.STREET_NAME),
                    trim(b.POST_TYPE),
                    trim(b.POST_DIRECTIONAL),
                    trim(b.POST_MODIFIER)
                ) as _STNAME_LABEL
            FROM(
                SELECT
                    a.segmentid,
                    b.B7SC,
                    a.stname_label
                FROM cscl.centerline a
                JOIN cscl.segment_lgc b
                ON a.segmentid = b.segmentid
                WHERE b.PREFERRED_LGC_FLAG='Y'
            ) a JOIN cscl.streetname b 
            ON a.B7SC=b.B7SC
            WHERE b.PRINCIPAL_FLAG='Y'
        ) a WHERE trim(stname_label) != _stname_label
    ) TO STDOUT DELIMITER ',' CSV HEADER;" > stname_label.csv
    rm ETL.gdb.zip
)

# Upload to Spaces
Upload stname_label latest
Upload stname_label $DATE