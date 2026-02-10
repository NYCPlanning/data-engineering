from dcpy.lifecycle import config
from dcpy.models.product.metadata import OrgMetadata


def load(**kwargs) -> OrgMetadata:
    md_path = (
        kwargs.get("org_md_path_override")
        or config.CONF["product_metadata"]["repo_path"]
    )
    return OrgMetadata.from_path(
        md_path,
        template_vars=kwargs,
    )
