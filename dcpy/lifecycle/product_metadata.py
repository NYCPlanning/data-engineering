from pathlib import Path
from dcpy.models.product.metadata import OrgMetadata
from dcpy.lifecycle import config


def load_org_md(org_md_path_override: Path | None = None, **template_vars):
    default_org_md_path = config.CONF["product_metadata_path"]
    return OrgMetadata.from_path(
        org_md_path_override or default_org_md_path, template_vars=template_vars
    )
