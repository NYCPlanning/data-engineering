"""EDDE Packager - Runs all packaging steps for EDDE data product."""

import subprocess
import sys
from pathlib import Path


def run_packaging():
    """Run all EDDE packaging steps in order.

    Steps:
    1. Generate site configuration files
    2. Generate change-over-time datasets
    3. Generate resolved pages and tables for equity explorer
    """
    packager_dir = Path(__file__).parent

    print("=" * 80)
    print("EDDE PACKAGING - Step 1: Generating site configuration files")
    print("=" * 80)
    site_conf_script = packager_dir / "site_conf_templates" / "package_site_conf.py"
    result = subprocess.run([sys.executable, str(site_conf_script)], cwd=site_conf_script.parent)
    if result.returncode != 0:
        print(f"ERROR: Site conf generation failed with exit code {result.returncode}")
        sys.exit(result.returncode)

    print("\n" + "=" * 80)
    print("EDDE PACKAGING - Step 2: Generating change-over-time datasets")
    print("=" * 80)
    change_over_time_script = packager_dir / "change_over_time" / "run_all.py"
    result = subprocess.run([sys.executable, str(change_over_time_script)], cwd=change_over_time_script.parent)
    if result.returncode != 0:
        print(f"ERROR: Change-over-time generation failed with exit code {result.returncode}")
        sys.exit(result.returncode)

    print("\n" + "=" * 80)
    print("EDDE PACKAGING - Step 3: Generating resolved pages and tables")
    print("=" * 80)
    resolved_pages_script = packager_dir / "resolved_pages_and_tables" / "generate.py"
    result = subprocess.run([sys.executable, str(resolved_pages_script)], cwd=resolved_pages_script.parent)
    if result.returncode != 0:
        print(f"ERROR: Resolved pages and tables generation failed with exit code {result.returncode}")
        sys.exit(result.returncode)

    print("\n" + "=" * 80)
    print("EDDE PACKAGING - All steps completed successfully!")
    print("=" * 80)


if __name__ == "__main__":
    run_packaging()
