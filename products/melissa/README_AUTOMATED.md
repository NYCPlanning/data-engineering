# Automated Melissa Geocoding

This guide helps you run the Melissa geocoding process in an automated, end-to-end fashion using Docker.

## Prerequisites

- **Docker Desktop** installed and running on Windows
- **WSL2** (Windows Subsystem for Linux) enabled
- Access to the Melissa data file on S3

## Quick Start

### Step 1: Create a Working Directory

Open WSL terminal and create a directory for your geocoding work:

```bash
# Create directory in your Windows filesystem (accessible from File Explorer)
mkdir -p /mnt/c/melissa-geocoding
cd /mnt/c/melissa-geocoding
```

This creates a folder at `C:\melissa-geocoding` on your Windows machine.

### Step 2: Download the Automation Script

Download the script from the repository:

```bash
wget https://raw.githubusercontent.com/NYCPlanning/data-engineering/main/products/melissa/run_geocode_automated.sh
chmod +x run_geocode_automated.sh
```

**OR** create the script manually by copying the content from `run_geocode_automated.sh` in this directory.

### Step 3: Run the Geocoder

Execute the Docker container with your S3 file URL:

```bash
docker run --rm \
  -v /mnt/c/melissa-geocoding:/data \
  nycplanning/build-geosupport:latest \
  bash /data/run_geocode_automated.sh https://edm-recipes.nyc3.digitaloceanspaces.com/tmp/melissa_20260206.txt
```

**Replace the S3 URL** with your actual Melissa data file URL.

### Step 4: Access Your Results

Once complete, find your geocoded file in the working directory:

- **From Windows File Explorer**: `C:\melissa-geocoding\`
- **From WSL**: `/mnt/c/melissa-geocoding/`

The output file will be named automatically (e.g., `melissa_20260206_geocoded.txt`).

## What the Script Does

1. **Installs dependencies**: `usaddress`, `python-geosupport`, `duckdb`, `pandas`
2. **Clones repository**: Gets the geocoding pipeline from the data-engineering repo
3. **Downloads input**: Fetches your Melissa data file from S3
4. **Runs the pipeline**: Loads the input plus the checked-in `data/melissa_corrections.csv`
   and `data/melissa_outsideofnyc.csv` reference files into DuckDB, geocodes every
   distinct address with NYC Geosupport (Functions 1E, 1A, and AP), merges in
   corrections, and writes the final CSV
5. **Writes output**: Saves geocoded results to the mounted directory

## Alternative: Running from PowerShell/CMD

If you prefer to run from Windows PowerShell or Command Prompt:

```powershell
# Create working directory
mkdir C:\melissa-geocoding
cd C:\melissa-geocoding

# Download script (using curl or browser)
curl -O https://raw.githubusercontent.com/NYCPlanning/data-engineering/main/products/melissa/run_geocode_automated.sh

# Run Docker
docker run --rm -v C:\melissa-geocoding:/data nycplanning/build-geosupport:latest bash /data/run_geocode_automated.sh https://edm-recipes.nyc3.digitaloceanspaces.com/tmp/melissa_20260206.txt
```

## Using Docker Desktop GUI

1. Open **Docker Desktop**
2. Go to **Images** and pull `nycplanning/build-geosupport:latest` if not already available
3. Click **Run** on the image
4. Configure:
   - **Container name**: `melissa-geocoding` (optional)
   - **Volumes**:
     - Host path: `C:\melissa-geocoding`
     - Container path: `/data`
   - **Command**:
     ```
     bash /data/run_geocode_automated.sh https://edm-recipes.nyc3.digitaloceanspaces.com/tmp/melissa_20260206.txt
     ```
5. Click **Run**
6. Monitor progress in the **Logs** tab

## Troubleshooting

### "Permission denied" errors
Make sure the script is executable:
```bash
chmod +x run_geocode_automated.sh
```

### "File not found" errors
Verify your volume mount path:
- Windows paths use backslashes: `C:\melissa-geocoding`
- Docker/WSL paths use forward slashes: `/mnt/c/melissa-geocoding` or `C:/melissa-geocoding`

### Container runs but no output file
- Check the container logs for errors
- Verify the S3 URL is accessible
- Ensure the volume mount is correctly configured

### Large files taking too long
Geocoding runs one Geosupport call per CPU core in parallel (`multiprocessing.Pool`),
committing results in chunks (`--chunk-size`, default 5000) to the persistent
DuckDB file as it goes. If a run is interrupted, just rerun the same command --
it reuses the same `.duckdb` file by default and automatically skips addresses
that were already geocoded, so at most one chunk's worth of work is lost.

## Technical Details

### Input Format
The input file should be pipe-delimited, with columns matching the Melissa vendor
data (`address`, `suite`, `city`, `state`, `zip`, `plus4`, `crrt`, `updatedate`, ...).
Column names are lowercased automatically.

### Output Format
The output includes all input columns, plus `hnum`/`sname` (parsed house number and
street name) and 35 geocode columns from Geosupport Functions 1E, 1A, and AP --
matching `Melissa_Geocoded_Layout_And_Field_Source.csv` exactly (corrected
borough/house number/street name, F1/F1A/FAP normalized house number and street
name, centerline and address-point coordinates, BBL, BIN, CD, NTA, and each
function's GRC/reason code/message). See `pipeline.py` for the full column list.

### Data Filtering
- Addresses matching `data/melissa_outsideofnyc.csv` are excluded before geocoding
- Addresses matching `data/melissa_corrections.csv` are re-geocoded using the
  corrected house number/street/borough, and those results are preferred over the
  original address's geocode result wherever both exist
- Rows where Geosupport could not geocode the address are still included, with the
  relevant GRC/message columns populated (including GRC 71, "address not found in
  NYC" -- this is intentionally not filtered out, to match the reference pipeline)

### Alternative: loading via `dcp_load_recipe`

Instead of the S3-URL + local-file flow above, `recipe.yml` in this directory
declares the same three inputs (`melissa_input`, `melissa_corrections`,
`melissa_outsideofnyc`) as `edm.private` datasets, following the same convention
`products/lift` uses for `dcas_lift.csv`. With direnv loaded and the files pushed
to the private bucket:

```bash
cd products/melissa
eval "$(direnv export bash)"
dcp_load_recipe   # plans + loads recipe.yml into a local .duckdb file
python3 pipeline.py melissa_geocoded.csv \
    --skip-load \
    --db-path melissa_v1.duckdb \
    --schema "$BUILD_ENGINE_SCHEMA"
```

## Support

For issues or questions, contact the Data Engineering team or file an issue in the data-engineering repository.
