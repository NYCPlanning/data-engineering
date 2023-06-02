#!/bin/bash
source bash/config.sh

function sql {
    shift;
    psql $BUILD_ENGINE $@
}

function cpdb_archive {
    shift;
    case $1 in
    --all)
        psql $BUILD_ENGINE\
        --no-align \
        --tuples-only \
        --field-separator ' ' \
        --pset footer=off \
        -c "
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        AND table_name not in (
            'geography_columns', 
            'geometry_columns', 
            'raster_columns', 
            'raster_overviews', 
            'spatial_ref_sys'
        );
        " | while read -a Record ; do
            local table_name="${Record[0]}"
            archive public.$table_name cpdb.$table_name
        done
        wait
        echo "Archive Complete"
        ;;
    *) archive $@ ;;
    esac
}

function cpdb_upload {
    local branchname=$(git rev-parse --symbolic-full-name --abbrev-ref HEAD)
    local DATE=$(date "+%Y-%m-%d")
    local SPACES="spaces/edm-publishing/db-cpdb/$branchname"
    local HASH=$(git describe --always)
    mc rm -r --force $SPACES/latest
    mc rm -r --force $SPACES/$DATE
    mc rm -r --force $SPACES/$HASH
    mc cp --attr acl=private -r output $SPACES/latest
    mc cp --attr acl=private -r output $SPACES/$DATE
    mc cp --attr acl=private -r output $SPACES/$HASH
}

function share {
    shift;
    case $1 in
        --help )
            echo
            echo "This command will generate a sharable url for a private file (Expiration 7 days)"
            echo "Usage: ./cpdb.sh share {{ branch }} {{ version }} {{ filename }}" 
            echo "e.g. : ./cpdb.sh share main latest output.zip"
            echo
        ;;
        *)
            local branch=${1:-main}
            local version=${2:-latest}
            local file=${3:-output.zip}
            mc share download spaces/edm-publishing/db-cpdb/$branch/$version/output/$file
        ;;
    esac
}

case $1 in 
    dataloading ) ./bash/01_dataloading.sh ;;
    attribute ) ./bash/02_build_attribute.sh ;;
    adminbounds ) ./bash/03_adminbounds.sh ;;
    analysis ) ./bash/04_analysis.sh ;;
    export ) ./bash/05_export.sh ;;
    archive ) cpdb_archive $@ ;;
    upload ) cpdb_upload ;;
    share ) share $@ ;;
    sql) sql $@;;
esac
