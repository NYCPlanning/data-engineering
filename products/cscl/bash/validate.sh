#!/bin/bash
source ../../bash/utils.sh
set_error_traps

error="f"

(
    cd output

    for file in ManhattanLion.dat BrooklynLion.dat BronxLion.dat QueensLion.dat StatenIslandLion.dat; do
        counts_of_row_lengths=$(awk '{print length}' $file | sort -g | uniq -c)
        if [ $(wc -l <<< "$counts_of_row_lengths") -gt 1 ]; then
            echo "Not all rows have length 400. See counts below"
            for line in "$counts_of_row_lengths"; do
                echo "$line"
            done
            error="t"
        else
            echo "All $(wc -l $file | awk '{print $1}') rows in $file have length 400"
        fi
    done
)

if [ "$error" = "t" ]; then
    exit 1
fi

echo "Validation passed"
