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
wget https://raw.githubusercontent.com/NYCPlanning/data-engineering/main/experimental/melissa/run_geocode_automated.sh
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

1. **Installs dependencies**: `usaddress` and `python-geosupport`
2. **Clones repository**: Gets the geocoding scripts from the data-engineering repo
3. **Downloads input**: Fetches your Melissa data file from S3
4. **Geocodes addresses**: Processes all addresses using NYC Geosupport
5. **Filters results**: Removes non-NYC addresses from output
6. **Writes output**: Saves geocoded results to the mounted directory

## Alternative: Running from PowerShell/CMD

If you prefer to run from Windows PowerShell or Command Prompt:

```powershell
# Create working directory
mkdir C:\melissa-geocoding
cd C:\melissa-geocoding

# Download script (using curl or browser)
curl -O https://raw.githubusercontent.com/NYCPlanning/data-engineering/main/experimental/melissa/run_geocode_automated.sh

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
The geocoding process can take several hours for large datasets. The script will show progress updates every 1000 rows.

## Technical Details

### Input Format
The input file should be pipe-delimited with these columns:
- `Address`: Street address
- `City`: City/neighborhood name
- `State`: State abbreviation
- `Zip`: ZIP code

### Output Format
The output includes all input columns plus:
- `bbl`: Borough-Block-Lot identifier
- `boro_code`: Borough code (1-5)
- `bin`: Building Identification Number
- `latitude`: Latitude coordinate
- `longitude`: Longitude coordinate
- `ct2020`: 2020 Census Tract
- `cb2020`: 2020 Census Block
- `bctcb2020`: Combined borough-census tract-block
- `grc`: Geosupport Return Code
- `message`: Geosupport message

### Data Filtering
- Non-NYC addresses are automatically filtered out
- Addresses with unparseable components are included with empty geocoding fields
- Failed geocodes are included with error messages in the `message` field

## Support

For issues or questions, contact the Data Engineering team or file an issue in the data-engineering repository.
