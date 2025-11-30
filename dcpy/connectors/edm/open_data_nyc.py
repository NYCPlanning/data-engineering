import re
import textwrap
import time
from pathlib import Path

import dcpy.models.product.dataset.metadata as md
from dcpy.connectors.registry import VersionedConnector
from dcpy.connectors.socrata import publish as soc_pub
from dcpy.lifecycle import product_metadata
from dcpy.utils.logging import logger


class OpenDataConnector(VersionedConnector):
    conn_type: str = "open_data"
    SOCRATA_DOMAIN: str = "data.cityofnewyork.us"
    PRE_PUBLISH_SLEEP_SECS: int = 120

    def list_versions(self, key: str, *, sort_desc: bool = True, **_) -> list[str]:
        return [self.get_latest_version(key)]

    def push_versioned(self, key: str, version: str, **kwargs) -> dict:
        product, dataset, destination_id = key.split(".")
        metadata = product_metadata.load(version=version)
        dataset_metadata = metadata.product(product).dataset(dataset)

        return {
            "result": self.distribute_dataset(
                metadata=dataset_metadata,
                dataset_destination_id=destination_id,
                dataset_package_path=kwargs["dataset_package_path"],
                publish=bool(kwargs.get("publish")),
                metadata_only=bool(kwargs.get("metadata_only")),
            )
        }

    def pull(self, key: str, destination_path: Path, **kwargs) -> dict:
        raise Exception("Not implemented yet")

    def pull_versioned(
        self, key: str, version: str, destination_path: Path, **kwargs
    ) -> dict:
        raise Exception("Not implemented yet")

    def get_latest_version(self, key: str, **kwargs) -> str:
        """Parses the latest version from the description on the OpenData page.

        Unfortunately, we couldn't find anywhere else to stash the version (e.g. in the page metadata)
        so we just append " Current version: [version-string]" to the end of the description.
        """
        product, dataset, destination_id = key.split(".")
        metadata = product_metadata.load(version="dummy")  # I don't love this...
        four_four = (
            metadata.product(product)
            .dataset(dataset)
            .get_destination(destination_id)
            .custom.get("four_four")
        )
        description = soc_pub.Dataset(
            four_four=four_four, socrata_domain=self.SOCRATA_DOMAIN
        ).get_description()

        match = re.search(r" Current version: ([A-Za-z0-9\-_.]+)$", description)
        return match.group(1) if match else ""

    def distribute_dataset(
        self,
        *,
        metadata: md.Metadata,
        dataset_destination_id: str,
        dataset_package_path: Path,
        publish: bool = False,
        metadata_only: bool = False,
    ):
        """Push a dataset and sync metadata."""
        logger.info(
            f"Pushing dataset {metadata.id} -> {dataset_destination_id} from {dataset_package_path}"
        )
        socrata_dest = soc_pub.SocrataDestination(metadata, dataset_destination_id)
        dataset = soc_pub.Dataset(
            socrata_domain=self.SOCRATA_DOMAIN, four_four=socrata_dest.four_four
        )

        overridden_attachments = [  # we really just care about the overridden filenames
            metadata.calculate_destination_metadata(
                file_id=attachment_id, destination_id=dataset_destination_id
            )
            for attachment_id in socrata_dest.attachment_ids
        ]

        rev = dataset.create_replace_revision()

        attachments_metadata = [
            rev.upload_attachment(
                dest_file_name=attachment.file.filename,
                path=dataset_package_path
                / "attachments"
                / metadata.get_file_and_overrides(attachment.file.id).file.filename,
            )
            for attachment in overridden_attachments
        ]

        overridden_dataset_md = metadata.calculate_destination_metadata(
            file_id=socrata_dest.dataset_file_id, destination_id=dataset_destination_id
        )
        rev.patch_metadata(
            attachments=attachments_metadata,
            metadata=soc_pub.Socrata.Inputs.DatasetMetadata.from_dataset_attributes(
                overridden_dataset_md.dataset.attributes
            ),
        )

        package_dataset_file_path = (
            dataset_package_path
            / "dataset_files"
            / metadata.get_file_and_overrides(
                socrata_dest.dataset_file_id
            ).file.filename
        )  # TODO: this isn't the right place for this calculation. Move to lifecycle.package.

        if not metadata_only:
            data_source = None
            dataset_file = overridden_dataset_md.file
            if socrata_dest.is_unparsed_dataset:
                rev.push_blob(
                    package_dataset_file_path,
                    dest_filename=overridden_dataset_md.file.filename
                    or package_dataset_file_path.name,
                )
            elif dataset_file.type == "csv":
                data_source = rev.push_csv(package_dataset_file_path)
            elif dataset_file.type == "shapefile":
                data_source = rev.push_shp(package_dataset_file_path)
            elif dataset_file.type == "xlsx":
                data_source = rev.push_xlsx(package_dataset_file_path)
            else:
                raise Exception(f"Pushing unsupported file type: {dataset_file.type}")

            if not socrata_dest.is_unparsed_dataset and data_source:
                try:
                    data_source.push_socrata_column_metadata(
                        overridden_dataset_md.dataset.columns
                    )
                except Exception as e:
                    # Upating column Metadata is tricky, and there's still some work to be done
                    logger.error(
                        "Error Updating Column Metadata! However, the Dataset File was uploaded "
                        f"and the revision can still be applied manually, here:\n    {rev.page_url}\n"
                        f"Error:\n{textwrap.indent(str(e), '    ')}"
                    )
                    raise Exception(
                        f"Error publishing {metadata.attributes.display_name} - destination: {dataset_destination_id}. Revision: {rev.revision_num}.\n {str(e)}"
                    )

        if not publish:
            result = f"""Finished syncing product {metadata.attributes.display_name} to Socrata, but did not publish. Find revision {rev.revision_num}, and apply manually here {rev.page_url}"""
            logger.info(result)
            return result
        else:
            logger.info("Publishing")
            time.sleep(
                self.PRE_PUBLISH_SLEEP_SECS
            )  # Time to let the file fully process after upload
            rev.apply()
            logger.info(
                "Revision Applied. Polling for publication completion (this can take minutes)."
            )

            elapsed_secs = 0
            while not rev.fetch_default_metadata().closed_at:
                logger.info("Polling for completion.")
                time.sleep(5)
                elapsed_secs += 5
                if elapsed_secs > rev.SOCRATA_REVISION_APPLY_TIMEOUT_SECS:
                    raise Exception(
                        f"""waited {rev.SOCRATA_REVISION_APPLY_TIMEOUT_SECS} seconds for the Socrata \
                        revision to apply, but it didn't. Note: it may just be a very long running job.
                        Please investigate manually here: {rev.page_url}"""
                    )
            logger.info("Job Finished Successfully")

            dataset.discard_open_revisions()
            return f"Published {metadata.attributes.display_name} - destination: {dataset_destination_id}"
