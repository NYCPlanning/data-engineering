#!/bin/bash

# Expects two folders in current directory
#  output - contains outputs of build
#  prod - contains "production" 25a (or whatever version) for comparison
mkdir validation_output

total_records=0
total_mismatched=0
for file in ManhattanLion.dat BrooklynLion.dat BronxLion.dat QueensLion.dat StatenIslandLion.dat; do
    echo "Validating $file"

    n_records="$(cat output/$file | wc -l |  awk '{print $1}')"
    echo "Total records:      $n_records"
    total_records=$(($total_records + $n_records))
    mismatched_rows=$(comm -23 <(sort output/$file) <(sort prod/$file))
    
    n_mismatched=$(echo "$mismatched_rows" | wc -l | awk '{print $1}')
    echo "Mismatched records: $n_mismatched"
    total_mismatched=$(($total_mismatched + $n_mismatched))

    echo -e "$mismatched_rows" > validation_output/$file
    echo ""
done

echo "Comparison complete!"
echo "Total records:      $total_records"
echo "Mismatched records: $total_mismatched"
