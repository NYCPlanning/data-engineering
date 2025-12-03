#!/usr/bin/env python3

import os
import subprocess
import sys
from pathlib import Path

from partitions import build_partition_defs, distribute_partition_defs
from dagster import DagsterInstance
from assets import ingest_partition_def

dagster_dir = Path(__file__).parent

# Set DAGSTER_HOME to a directory in the user's home
dagster_home = Path.home() / ".dagster"
dagster_home.mkdir(exist_ok=True)
os.environ["DAGSTER_HOME"] = str(dagster_home)


def setup_sample_partitions():
    """Automatically create sample partitions if they don't exist"""
    try:

        instance = DagsterInstance.get()

        # Check if we already have partitions (avoid recreating)
        partition_name = ingest_partition_def.name
        if partition_name:
            existing_ingest = instance.get_dynamic_partitions(partition_name)
            if existing_ingest:
                print(
                    f"📥 Found existing partitions, skipping setup ({len(existing_ingest)} ingest partitions)"
                )
                return

        print("🚀 Setting up sample partitions...")

        # 1. Ingest partitions
        ingest_partitions = ["nightly_qa", "main", "v2024_12_01", "v2025_01_15"]
        if partition_name:
            instance.add_dynamic_partitions(partition_name, ingest_partitions)
        print(f"✅ Added {len(ingest_partitions)} ingest partitions")

        # 2. Build partitions for first 3 products
        build_partitions = [
            "2025.1.1-1_initial_build",
            "2025.1.1-2_fix_validation",
            "2025.1.1-3_final_build",
            "2025.1.2-1_hotfix",
            "2025.2.0-1_new_features",
        ]

        sample_products = ["pluto_cbbr", "cdbg", "ceqr"]
        for product in sample_products:
            if product in build_partition_defs:
                build_name = build_partition_defs[product].name
                if build_name:
                    instance.add_dynamic_partitions(build_name, build_partitions)

        # 3. Distribution partitions
        distribute_partitions = ["2024.4", "2025.1", "2025.2"]
        for product in sample_products:
            if product in distribute_partition_defs:
                distribute_name = distribute_partition_defs[product].name
                if distribute_name:
                    instance.add_dynamic_partitions(
                        distribute_name, distribute_partitions
                    )
        
        # 4. Special case: Pluto multi-dimensional partitions (year + quarter)
        # Add sample years to the year partition
        pluto_years = ["2023", "2024", "2025"]
        try:
            instance.add_dynamic_partitions("pluto_year", pluto_years)
            print(f"✅ Added {len(pluto_years)} Pluto year partitions")
        except Exception as e:
            print(f"ℹ️  Pluto year partitions: {e}")
        
        # Quarter partitions are static (1,2,3,4) so no need to add them

        print(f"✅ Sample partitions created for {len(sample_products)} products!")

    except Exception as e:
        print(f"⚠️  Could not setup partitions: {e}")
        print("   (This is normal if running for the first time)")


def main():
    # Setup partitions before starting the server
    setup_sample_partitions()

    if len(sys.argv) > 1 and sys.argv[1] == "dev":
        cmd = ["dagster", "dev", "-f", "definitions.py"]
    else:
        cmd = ["dagster-webserver", "-f", "definitions.py"]

    subprocess.run(cmd, cwd=dagster_dir)


if __name__ == "__main__":
    main()
