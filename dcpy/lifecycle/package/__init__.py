from pathlib import Path

# from .assemble import assemble_package, pull_destination_package_files
# from .validate import validate as validate_package
from dcpy.lifecycle import config

RESOURCES_PATH = Path(__file__).parent / "resources"
ASSEMBLY_DIR = config.local_data_path_for_stage("package.assemble")

# __all__ = [
#     "assemble_package",
#     "ASSEMBLY_DIR",
#     "pull_destination_package_files",
#     "validate_package",
# ]


# class PackagePullResult:
#     PulledPath: Path


# def pull(
#     from_destination, dataset_md, validation_conf: ValidationArgs = {}
# ) -> list[PackagePullResult]:
#     packaging_pull_connector_name = config.stage_config("package")["default_connector"][
#         "pull"
#     ]
#     packaging_pull_connector = connector_registry.connectors[
#         packaging_pull_connector_name
#     ]

#     pull_results = packaging_pull_connector.pull()

#     if validate:
#         if not (validation_conf.get("skip_validation") or metadata_only):
#             package_validations = package.validate_package(package_path=package_path)
#             if package_validations.get_errors_list():
#                 if validation_conf.get("ignore_validation_errors"):
#                     logger.warning("Package Errors Found! But continuing distribute")
#                     logger.warning(package_validations.make_errors_table())
#                 else:
#                     logger.error("Errors Found! Aborting distribute")
#                     logger.error(package_validations.make_errors_table())
#                     raise Exception(package_validations.make_errors_table())
#             else:
#                 logger.info("\nFinished Packaging. Beginning Distribution.")
