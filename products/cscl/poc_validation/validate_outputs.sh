#!/bin/bash

# Compares dev build output files against production files and writes per-file diff results.
#
# NOTE: This is what the build runs (see cscl_build.yml), and the QA app reads the
# validation_output/ files it writes. It needs local copies of both output/ and
# .data/prod/, so it only works during or just after a build.
# poc_validation/run_validation.py runs the same comparison against a build that has
# already been published, streaming both sides from S3.
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
    # Outputs marked compare_file: false in recipe.yml are never pulled, so there is
    # nothing to compare against. The LDF is one: see qa__ldf_summary instead.
    if [ ! -f ".data/prod/$file" ]; then
        echo "Skipping $file, no production file to compare against"
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
