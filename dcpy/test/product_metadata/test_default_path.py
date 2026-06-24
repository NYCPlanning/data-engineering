"""Test that PRODUCT_METADATA_REPO_PATH defaults to the in-repo copy.

This is the highest-value check for issue #2436: callers no longer need the
env var set; the vendored product-metadata/ directory is used automatically.
"""

from pathlib import Path


def test_default_path_resolves_to_in_repo_copy():
    """PRODUCT_METADATA_REPO_PATH defaults to the vendored in-repo directory.

    Relies on PRODUCT_METADATA_REPO_PATH being UNSET in the environment.
    If the env var is set externally, this test still checks the path exists
    and points at a valid product-metadata layout.

    The spec change (dcpy/configuration.py): replace the "no value" warning
    with an inline default of Path(__file__).parent.parent / "product-metadata".
    """
    import dcpy.configuration as config

    path = Path(config.PRODUCT_METADATA_REPO_PATH)

    assert path.exists(), (
        f"PRODUCT_METADATA_REPO_PATH resolves to non-existent path: {path}. "
        "The in-repo product-metadata/ directory is missing."
    )
    assert path.is_dir(), f"PRODUCT_METADATA_REPO_PATH is not a directory: {path}"

    # Confirm it looks like a product-metadata repo: has the two required top-level files
    assert (path / "metadata.yml").exists(), (
        f"Expected metadata.yml at {path}/metadata.yml"
    )
    assert (path / "data_dictionary.yml").exists(), (
        f"Expected data_dictionary.yml at {path}/data_dictionary.yml"
    )
    assert (path / "products").is_dir(), f"Expected products/ dir at {path}/products"
    assert (path / "snippets").is_dir(), f"Expected snippets/ dir at {path}/snippets"


def test_default_path_is_inside_repo():
    """When no env var is set, the default path is inside the data-engineering repo.

    This asserts the specific invariant from the spec: the default is
    Path(__file__).parent.parent / "product-metadata" relative to configuration.py,
    which is the repo root / "product-metadata".
    """
    import os

    # Only assert the in-repo location when the env var is genuinely absent
    if "PRODUCT_METADATA_REPO_PATH" in os.environ:
        import pytest

        pytest.skip("PRODUCT_METADATA_REPO_PATH is set; skipping in-repo default check")

    import dcpy.configuration as config

    resolved = Path(config.PRODUCT_METADATA_REPO_PATH)
    config_file = Path(config.__file__)
    expected = config_file.parent.parent / "product-metadata"

    assert resolved == expected, (
        f"Default path {resolved} should equal {expected} (repo root / product-metadata)"
    )
