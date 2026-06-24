#!/bin/bash
# Automated Melissa Geocoding Script
# Runs inside nycplanning/build-geosupport:latest container
# Usage: ./run_geocode_automated.sh <s3-url>

set -e

S3_URL=$1
if [ -z "$S3_URL" ]; then
    echo "Error: S3 URL is required"
    echo "Usage: $0 <s3-url>"
    echo "Example: $0 https://edm-recipes.nyc3.digitaloceanspaces.com/tmp/melissa_20260206.txt"
    exit 1
fi

echo "========================================="
echo "  Melissa Geocoding Automation"
echo "========================================="
echo "S3 URL: $S3_URL"
echo ""

# Extract filename from URL and generate output filename
INPUT_FILENAME=$(basename "$S3_URL")
OUTPUT_FILENAME="${INPUT_FILENAME%.txt}_geocoded.txt"

echo "Step 1/5: Installing Python dependencies..."
pip3 install -q usaddress python-geosupport

echo "Step 2/5: Cloning data-engineering repository..."
cd /tmp
rm -rf data-engineering 2>/dev/null || true
git clone --quiet --depth 1 --branch main https://github.com/NYCPlanning/data-engineering.git

echo "Step 3/5: Downloading input file from S3..."
cd /data
wget -q --show-progress "$S3_URL" -O "$INPUT_FILENAME"
echo "  Downloaded: $INPUT_FILENAME"

echo "Step 4/5: Running geocoder..."
echo "  Input:  $INPUT_FILENAME"
echo "  Output: $OUTPUT_FILENAME"
echo "  (Non-NYC addresses will be filtered out)"
echo ""

python3 /tmp/data-engineering/experimental/melissa/geocode.py \
    "$INPUT_FILENAME" \
    "$OUTPUT_FILENAME"

echo ""
echo "Step 5/5: Cleaning up..."
rm -rf /tmp/data-engineering

echo ""
echo "========================================="
echo "  Geocoding Complete!"
echo "========================================="
echo "Output file: $OUTPUT_FILENAME"
echo "Location: /data/$OUTPUT_FILENAME"
echo ""
echo "The geocoded file is now available in your mounted directory."
