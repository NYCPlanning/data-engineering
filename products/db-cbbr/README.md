# Community Board Budget Requests Database (CBBR)

The Community Board Budget Requests Database, a data product produced by the New York City Department of City Planning (DCP), is based on requests for future capital or expense projects submitted by each of NYC's 59 Community Boards to DCP.  DCP then disseminates this information to the Office of Management and Budget and each of NYC's agencies.  DCP adds value to the submitted budget requests by creating geometries where possible for requested projects, in the effort to map as many budget requests as possible.

The Community Board Budget Requests Database is a way to quickly and easily explore and learn about projects requested by NYC's 59 Community Boards.  It’s main purpose is to be a starting point for exploring potential projects and to better understand communities' perceived needs across NYC.  The spatial data provides an integrated view enabling a broad understanding of where communities have requested investments, and reveals opportunities for strategic neighborhood planning.

## Output files (FY2024)

- [cbbr_submissions.csv](https://raw.githubusercontent.com/NYCPlanning/db-cbbr/master/cbbr_build/output/FY2024/cbbr_export.csv)
- [cbbr_submissions_needgeoms.csv](https://raw.githubusercontent.com/NYCPlanning/db-cbbr/master/cbbr_build/output/FY2024/cbbr_submissions_needgeoms.csv)
- [cbbr_submissions_poly.csv](https://raw.githubusercontent.com/NYCPlanning/db-cbbr/master/cbbr_build/output/FY2024/cbbr_submissions_poly.csv)
- [cbbr_submissions_pts.csv](https://raw.githubusercontent.com/NYCPlanning/db-cbbr/master/cbbr_build/output/FY2024/cbbr_submissions_pts.csv)
- [cbbr_submissions_poly_shapefile.zip](https://raw.githubusercontent.com/NYCPlanning/db-cbbr/master/cbbr_build/output/FY2024/cbbr_submissions_poly_shapefile.zip)
- [cbbr_submissions_pts_shapefile.zip](https://raw.githubusercontent.com/NYCPlanning/db-cbbr/master/cbbr_build/output/FY2024/cbbr_submissions_pts_shapefile.zip)

### Limitations

The spatial data are not 100% reliable, accurate, or exhaustive

CBBR is primarily built for planning coordination and information purposes only

## Building Dataset

### Building Preparation

1. `cd cbbr_build` to navigate to the build directory
2. Create `cbbr_build/.env` and set environment variables `RECIPE_ENGINE`, `BUILD_ENGINE`, and `EDM_DATA`
3. In `cbbr_build/.version.env` set input dataset versions
4. Start a postgis docker container to create a local database:

    ```bash
    docker run --name <custom_container_name> -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgis/postgis
    ```

## Building Instructions

1. `./01_dataloading.sh` to load all source data into the postgresDB container
2. `./02_cbbr.sh` to normalize the agency and community district fields
3. `./03_spatial.sh` to geocode the dataset
4. `./04_export.sh` to export the dataset as csv
5. `./05_archive.sh` to archive the cbbr to EDM postgresDB

## Dev

### Dev Preparation

1. Create and/or activate a python virtual environment
2. `./dev_python_packages.sh` to install python packages

### Notes

- Dev, linting, formatting, and testing of python files requires the use of a local python interpreter with packages listed in `requirements.txt`, rather than the packages used during the build.
  - The build currently runs python files from within the `nycplanning/docker-geosupport` container.
- Since there is no devcontainer for this repo, local data building and dev rely on the local postgres service to run scripts with commands like `psql $BUILD_ENGINE -f $target_dir/$name.sql`.

### Test

1. Start a terminal within the Geosupport docker container:

    ```bash
    docker run -it --rm \
        -v $(pwd):/home/db-cbbr \
        -w /home/db-cbbr \
        --env-file .env \
        --network="host" \
        nycplanning/docker-geosupport:latest bash
    ```

2. Install pytest with `python3 -m pip install pytest`
3. Run python tests with `python3 -m pytest`


### Processing Manually Digitized Geometry Files

Typically, there are many records in the original CBBR Submissions source data that cannot/do not get programmatically geocoded by GeoSupport. This is usually do to insufficient location information or a particular record might not be applicable to a specific location (not a mappable project). To increase the number of mappable projects in the CBBR data, EDM set out to manually map records that were considered Priority A projects (that is projects identified as `Capital Projects` and which have been identified as a `Site` location).

In order to properly preprocess these corrections to the existing CBBR data, the following steps are necessary: 

1. Download the files (.shp and .gdb) from Sharepoint and bring them into QGIS.
2. Merge “like” geometries using the ‘Vector` -> `Data Management Tool` -> `Merge Vector Layers` being cognizant that original CBBR geometries are projected in CRS/EPSG: 4236 (Export the file as 4236). EDM digitizers used different methods of digitizing and some used CRS/EPSG: 2263.
    a. Note: you can merge Shapefiles and GDB files with like geometries 
3. Repeat step for Points, Polygons, and Lines
4. Export the files as CSV’s
    a. Right Click the file you want to export: `Export` -> `Save Features As…` 
    b. Create file name for each individual file (e.g. `cbbr_line_corrections`)
    c. Select `Layer Options` -> `Geometry` - `AS_WKT`
    d. Save the file(s) into the `db-cbbr/cbbr_build/cbbr_geom_corrections/processed/` folder 
5. Once that is done, the creation of the corrections files takes place programmatically.
