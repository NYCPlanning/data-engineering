import pandas as pd
from pathlib import Path

from dcpy.models.lifecycle.ingest import Config
from dcpy.connectors.edm import recipes
from dcpy.utils.logging import logger


def validate_against_existing_versions(ds: recipes.Dataset, filepath: Path) -> bool:
    """
    This function is called after a dataset has been preprocessed, just before archival
    It's called in the case that the version of the dataset in the config (either provided or calculated)
      already exists

    The last archived dataset with the same version is pulled in by pandas and compared to what was just processed
    If they are identical, the last archived dataset has its config updated to reflect that it was checked but not re-archived
    If they differ, the version is "patched" and a new patched version is archived
    """
    existing_config = recipes.try_get_config(ds)
    if not existing_config:
        logger.info(f"Dataset '{ds.key}' does not exist in recipes bucket")
        return True
    else:
        if isinstance(existing_config, Config):
            new = pd.read_parquet(filepath)
            comparison = recipes.read_df(ds)
            if new.equals(comparison):
                logger.info(
                    f"Dataset '{ds.key}' already exists and matches newly processed data"
                )
                return False
            else:
                raise FileExistsError(
                    f"Archived dataset '{ds.key}' already exists and has different data."
                )

        # if previous was archived with library, we both expect some potential slight changes
        # and are not able to update "freshness"
        else:
            logger.warning(
                f"previous version of '{ds.key}' archived is from library. cannot update freshness"
            )
            return False
