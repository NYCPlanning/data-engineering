from pathlib import Path

RESOURCES_PATH = Path(__file__).parent / "resources"

from .assemble import assemble_package, pull_destination_package_files
from .validate import validate as validate_package
from dcpy.lifecycle import config

ASSEMBLY_DIR = config.local_data_path_for_stage("package.assemble")

__all__ = [
    "assemble_package",
    "ASSEMBLY_DIR",
    "pull_destination_package_files",
    "validate_package",
]

class PackagePullResult:
    PulledPath: Path

def pull(destination, org_md, validation_conf: ValidationArgs = {}) -> list[PackagePullResult]:
    packaging_connector_name = config.stage_config("packaging")["default_connector"]

    pull_results = PackagePullResult
    dest_to_package_paths[d["destination_path"]] = connector_registry.connectors[
        packaging_connector_name
    ].pull(destination=d, org_metadata = org_md)

    if validate:
        if not (validation_conf.get("skip_validation") or metadata_only):
            package_validations = package.validate_package(package_path=package_path)
            if package_validations.get_errors_list():
                if validation_conf.get("ignore_validation_errors"):
                    logger.warning("Package Errors Found! But continuing distribute")
                    logger.warning(package_validations.make_errors_table())
                else:
                    logger.error("Errors Found! Aborting distribute")
                    logger.error(package_validations.make_errors_table())
                    raise Exception(package_validations.make_errors_table())
            else:
                logger.info("\nFinished Packaging. Beginning Distribution.")



class BytesPackageConnector:
    """Eventually, will just pull packaged files from EDM_Publishing.
    For now, if the package isn't present on the machine, it will pull the zipped package and
    re-assemble into our package format.
    """

    def pull(args):
        # Infer the package from the metadata
        package.pull_destination_package_files(
            local_package_path=package_path,
            source_destination_id=source_destination_id,
            dataset_metadata=dataset_md,
        )

        package.assemble_package(
            org_md=org_md,
            product=product,
            dataset=dataset,
            version=version,
            source_destination_id=source_destination_id,
            metadata_only=metadata_only,
        )
