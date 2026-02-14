#!/bin/bash

# Expects two folders in current directory
#  output - contains outputs of build
#  prod - contains "production" 25a (or whatever version) for comparison
mkdir validation_output

total_records=0
total_mismatched=0
for filepath in output/*; do
    file=$(basename "$filepath")
    if [[ "$file" =~ "zip" ]]; then
        continue
    fi
    echo "Validating $file"

    n_records="$(cat output/$file | wc -l |  awk '{print $1}')"
    echo "Total records:      $n_records"
    total_records=$(($total_records + $n_records))
    mismatched_rows=$(comm -23 <(sort output/$file) <(sort prod/$file))
    
    if [ -z "$mismatched_rows" ]; then
        n_mismatched=0
    else
        n_mismatched=$(echo "$mismatched_rows" | wc -l | awk '{print $1}')
    fi
    echo "Mismatched records: $n_mismatched"
    total_mismatched=$(($total_mismatched + $n_mismatched))

    echo -e "$mismatched_rows" > validation_output/$file
    echo ""
done

echo "Comparison complete!"
echo "Total records:      $total_records"
echo "Mismatched records: $total_mismatched"
