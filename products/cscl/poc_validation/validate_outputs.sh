#!/bin/bash

# Compares dev build output files against production files and writes per-file diff results.
#
# NOTE: This runs as the build's QA stage (recipe.yml stage_config builds.qa), and the QA
# app reads the validation_output/ files it writes.
# poc_validation/run_validation.py runs the same comparison against a build that has
# already been published, streaming both sides from S3.
#
# For each packaged file, performs a line-level comparison against the matching prod file
# and writes the mismatched rows to <validation dir>/<filename> - dev-only
# rows first, then (if any) a "--- ONLY IN PROD ---" section for rows prod has that dev is
# missing. Also writes a summary CSV (validation_summary.csv) with per-file prod row counts,
# dev-only/prod-only counts, and their total (mismatched_rows).
#
# mismatched_rows is a two-directional set difference (dev-only + prod-only), not just
# dev-only - comm -23 alone can't see rows prod has that dev is missing, which silently
# undercounts any file where dev is missing real content rather than producing spurious extra
# rows (found via Exception.txt: qa__diffs_exception found 4 real diffs - 3 prod-only, 1
# dev-only - while a dev-only-only comparison here reported just 1). Keep this in sync with
# poc_validation/run_validation.py's _compute_file_diff, which is meant to compute the same
# thing against a published build.
#
# Paths: see poc_validation/paths.py (same rules). Runs as the QA stage
# (dcpy lifecycle builds qa run), which sets PACKAGE_DIR and QA_DIR.
: "${PACKAGE_DIR:?run as the QA stage (dcpy lifecycle builds qa run)}"
: "${QA_DIR:?run as the QA stage (dcpy lifecycle builds qa run)}"
DEV_DIR=${DATASET_FILES_DIR:-$PACKAGE_DIR/dataset_files}
PROD_DIR=${PROD_FILES_DIR:-$QA_DIR/prod}
OUT_DIR=${VALIDATION_DIR:-$QA_DIR/validation_output}
mkdir -p "$OUT_DIR"

csv_file="$OUT_DIR/validation_summary.csv"
echo "filename,prod_row_count,mismatched_rows,dev_only_rows,prod_only_rows" > "$csv_file"

total_records=0
total_mismatched=0
# dataset_files may group files into subfolders (flat_files/, district_gdb/, ...)
for filepath in $(find "$DEV_DIR" -type f | sort); do
    file=$(basename "$filepath")
    if [[ "$file" =~ "zip" ]] || [[ -d "$filepath" ]]; then
        continue
    fi
    # Outputs marked compare_file: false in recipe.yml are never pulled, so there is
    # nothing to compare against. The LDF is one: see qa__ldf_summary instead.
    if [ ! -f "$PROD_DIR/$file" ]; then
        echo "Skipping $file, no production file to compare against"
        continue
    fi

    echo "Validating $file"

    prod_row_count="$(cat "$PROD_DIR/$file" | wc -l | awk '{print $1}')"
    echo "Total records:      $prod_row_count"
    total_records=$(($total_records + $prod_row_count))
    dev_sorted=$(mktemp)
    prod_sorted=$(mktemp)
    sort "$filepath" > "$dev_sorted"
    sort "$PROD_DIR/$file" > "$prod_sorted"
    dev_only_rows=$(comm -23 "$dev_sorted" "$prod_sorted")
    prod_only_rows=$(comm -13 "$dev_sorted" "$prod_sorted")
    rm -f "$dev_sorted" "$prod_sorted"

    if [ -z "$dev_only_rows" ]; then
        n_dev_only=0
    else
        n_dev_only=$(echo "$dev_only_rows" | wc -l | awk '{print $1}')
    fi
    if [ -z "$prod_only_rows" ]; then
        n_prod_only=0
    else
        n_prod_only=$(echo "$prod_only_rows" | wc -l | awk '{print $1}')
    fi
    n_mismatched=$(($n_dev_only + $n_prod_only))
    echo "Mismatched records: $n_mismatched (dev-only: $n_dev_only, prod-only: $n_prod_only)"
    total_mismatched=$(($total_mismatched + $n_mismatched))

    {
        [ -n "$dev_only_rows" ] && echo -e "$dev_only_rows"
        if [ -n "$prod_only_rows" ]; then
            echo "--- ONLY IN PROD (missing from dev) ---"
            echo -e "$prod_only_rows"
        fi
    } > "$OUT_DIR/$file"
    echo "$file,$prod_row_count,$n_mismatched,$n_dev_only,$n_prod_only" >> "$csv_file"
    echo ""
done

echo "Comparison complete!"
echo "Total records:      $total_records"
echo "Mismatched records: $total_mismatched"
