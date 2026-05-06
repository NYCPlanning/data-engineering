#!/usr/bin/env python3
"""
Copy CSCL data products to the edm-qaqc database for external consumption.

This script copies:
1. lion_outputs seed -> edm-qaqc.app.cscl__lion_outputs (full overwrite)
2. qa__diffs_all -> edm-qaqc.app.cscl__diffs_all (incremental by diff_run_date)

Usage:
    python copy_to_qaqc.py
"""

import sys

from dcpy.utils.postgres import PostgresClient


def copy_lion_outputs(source_client: PostgresClient, target_client: PostgresClient):
    """Copy lion_outputs seed to edm-qaqc.app.cscl__lion_outputs (full overwrite)."""
    print("Reading lion_outputs from source...")
    df = source_client.read_table_df("lion_outputs")

    print(f"Writing {len(df)} rows to cscl__lion_outputs...")
    target_client.insert_dataframe(
        df=df,
        table_name="cscl__lion_outputs",
        if_exists="replace",
    )
    print(f"✓ Copied {len(df)} rows to cscl__lion_outputs")


def copy_diffs_all(source_client: PostgresClient, target_client: PostgresClient):
    """Copy qa__diffs_all to edm-qaqc.app.cscl__diffs_all (incremental by diff_run_date)."""
    print("Reading qa__diffs_all from source...")
    source_df = source_client.read_table_df("qa__diffs_all")

    if source_df.empty:
        print("  Source table is empty, nothing to copy")
        return

    # Get existing diff_run_dates from target if table exists
    if target_client.table_or_view_exists("cscl__diffs_all"):
        print("Checking for existing diff_run_dates in target...")
        existing_df = target_client.read_table_df(
            "cscl__diffs_all", columns=["diff_run_date"]
        )
        existing_dates = set(existing_df["diff_run_date"].unique())
        print(f"  Found {len(existing_dates)} existing diff_run_date(s) in target")

        # Filter out rows with dates that already exist
        new_df = source_df[~source_df["diff_run_date"].isin(existing_dates)]

        if new_df.empty:
            print("  No new records to append (all diff_run_dates already exist)")
            return

        print(f"Appending {len(new_df)} new rows to cscl__diffs_all...")
        target_client.insert_dataframe(
            df=new_df,
            table_name="cscl__diffs_all",
            if_exists="append",
        )
        print(f"✓ Appended {len(new_df)} new rows to cscl__diffs_all")
    else:
        # Table doesn't exist, create it with all data
        print(f"Creating cscl__diffs_all with {len(source_df)} rows...")
        target_client.insert_dataframe(
            df=source_df,
            table_name="cscl__diffs_all",
            if_exists="replace",
        )
        print(f"✓ Created cscl__diffs_all with {len(source_df)} rows")


def main():
    """Main execution function."""
    print("Starting CSCL data copy to edm-qaqc...")
    print("Source database: db-cscl, schema: main")
    print("Target database: edm-qaqc, schema: app")
    print()

    try:
        # Create client for source data (main schema in db-cscl)
        source_client = PostgresClient(schema="main", database="db-cscl")

        # Create client for target data (app schema in edm-qaqc)
        target_client = PostgresClient(schema="app", database="edm-qaqc")

        # Copy lion_outputs
        print("1. Copying lion_outputs seed...")
        copy_lion_outputs(source_client, target_client)
        print()

        # Copy diffs_all
        print("2. Copying qa__diffs_all (incremental)...")
        copy_diffs_all(source_client, target_client)
        print()

        print("✓ All data copied successfully!")

    except Exception as e:
        print(f"✗ Error: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
