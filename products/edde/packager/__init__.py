"""EDDE Packager - Runs all packaging steps for EDDE data product."""

from dcpy.utils.logging import logger
from packager.change_over_time.run_all import run_all_conversions
from packager.district_xlsx.generate import main as generate_district_xlsx
from packager.resolved_pages_and_tables.generate import main as generate_resolved_pages
from packager.site_conf_templates.package_site_conf import package_site_conf


def run_packaging():
    """Run all EDDE packaging steps in order.

    Steps:
    1. Generate site configuration files
    2. Generate change-over-time datasets
    3. Generate resolved pages and tables for equity explorer
    4. Generate district XLSX files
    """
    logger.info("=" * 80)
    logger.info("EDDE PACKAGING - Step 1: Generating site configuration files")
    logger.info("=" * 80)
    package_site_conf()

    logger.info("")
    logger.info("=" * 80)
    logger.info("EDDE PACKAGING - Step 2: Generating change-over-time datasets")
    logger.info("=" * 80)
    run_all_conversions()

    logger.info("")
    logger.info("=" * 80)
    logger.info("EDDE PACKAGING - Step 3: Generating resolved pages and tables")
    logger.info("=" * 80)
    generate_resolved_pages()

    logger.info("")
    logger.info("=" * 80)
    logger.info("EDDE PACKAGING - Step 4: Generating district XLSX files")
    logger.info("=" * 80)
    generate_district_xlsx()

    logger.info("")
    logger.info("=" * 80)
    logger.info("EDDE PACKAGING - All steps completed successfully!")
    logger.info("=" * 80)


if __name__ == "__main__":
    run_packaging()
