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
    dataloading ) python3 -m dcpy.lifecycle.builds.load recipe --recipe_path ./${2:-"recipe"}.yml ;;
    preprocessing ) ./bash/01_preprocessing.sh ;;
    attribute ) ./bash/02_build.sh ;;
    adminbounds ) ./bash/03_adminbounds.sh ;;
    analysis ) ./bash/04_analysis.sh ;;
    export ) ./bash/05_export.sh ;;
    archive ) cpdb_archive $@ ;;
    upload ) python3 -m dcpy.connectors.edm.publishing upload -p db-cpdb -a private ;;
    share ) share $@ ;;
    sql) sql $@;;
    build)
        recipe=$2
        shift 2
        python3 -m dcpy.lifecycle.builds.load recipe --recipe-path ${recipe}.yml $@
        export VERSION=$(yq .version recipe.lock.yml)
        ./bash/01_preprocessing.sh
        ./bash/02_build.sh
        ./bash/03_adminbounds.sh
        ./bash/04_analysis.sh
        ./bash/05_export.sh
        python3 -m dcpy.connectors.edm.publishing upload -p db-cpdb -a private
esac
