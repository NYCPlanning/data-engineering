#!/bin/bash

# Compares dev build output files against production files and writes per-file diff results.
#
# NOTE: This script is a legacy local tool. The primary method for generating diffs is
# poc_validation/run_validation.py, which streams files from S3 without requiring local
# copies and is used by both the nightly build and the QA app.
# Use this script only if you have local copies of both output/ and .data/prod/ already.
#
# For each file in output/, performs a line-level comparison against the matching file in
# .data/prod/ and writes the mismatched (dev-only) rows to output/validation_output/<filename>.
# Also writes a summary CSV (validation_summary.csv) with per-file prod row counts and
# mismatched row counts.
#
# Expects two folders in the current directory:
#  output/dataset_files/  - contains outputs of the current dev build
#  .data/prod/            - contains the production files to compare against
mkdir -p output/validation_output

csv_file="output/validation_output/validation_summary.csv"
echo "filename,prod_row_count,mismatched_rows" > "$csv_file"

total_records=0
total_mismatched=0
for filepath in output/dataset_files/*; do
    file=$(basename "$filepath")
    if [[ "$file" =~ "zip" ]] || [[ -d "$filepath" ]]; then
        continue
    fi
    echo "Validating $file"

    prod_row_count="$(cat .data/prod/$file | wc -l | awk '{print $1}')"
    echo "Total records:      $prod_row_count"
    total_records=$(($total_records + $prod_row_count))
    mismatched_rows=$(comm -23 <(sort output/dataset_files/$file) <(sort .data/prod/$file))
    
    if [ -z "$mismatched_rows" ]; then
        n_mismatched=0
    else
        n_mismatched=$(echo "$mismatched_rows" | wc -l | awk '{print $1}')
    fi
    echo "Mismatched records: $n_mismatched"
    total_mismatched=$(($total_mismatched + $n_mismatched))

    echo -e "$mismatched_rows" > output/validation_output/$file
    echo "$file,$prod_row_count,$n_mismatched" >> "$csv_file"
    echo ""
done

echo "Comparison complete!"
echo "Total records:      $total_records"
echo "Mismatched records: $total_mismatched"
