#!/bin/bash

# Expects two folders in current directory
#  output - contains outputs of build
#  .data/prod - contains "production" 25a (or whatever version) for comparison
mkdir output/validation_output

csv_file="output/validation_output/validation_summary.csv"
echo "filename,prod_row_count,mismatched_rows" > "$csv_file"

total_records=0
total_mismatched=0
for filepath in output/*; do
    file=$(basename "$filepath")
    if [[ "$file" =~ "zip" ]] || [[ -d "$filepath" ]]; then
        continue
    fi
    echo "Validating $file"

    prod_row_count="$(cat .data/prod/$file | wc -l | awk '{print $1}')"
    echo "Total records:      $prod_row_count"
    total_records=$(($total_records + $prod_row_count))
    mismatched_rows=$(comm -23 <(sort output/$file) <(sort .data/prod/$file))
    
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
