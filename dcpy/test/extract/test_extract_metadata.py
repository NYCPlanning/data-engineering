import yaml

from dcpy.connectors.edm import recipes
from dcpy.extract import TEMPLATE_DIR, metadata


def test_validate_all_datasets():
    templates = [t for t in TEMPLATE_DIR.glob("*")]
    assert len(templates) > 0
    for file in templates:
        with open(file, "r") as f:
            s = yaml.safe_load(f)
        val = metadata.Template(**s)
        assert val


def test_jinja_templating():
    raw_template = metadata.read_template("dcp_atomicpolygons")
    assert isinstance(raw_template.source, recipes.ExtractConfig.Source.FileDownload)
    assert (
        raw_template.source.url
        == "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/nyap_.zip"
    )

    template = metadata.read_template("dcp_atomicpolygons", version="test")
    assert isinstance(template.source, recipes.ExtractConfig.Source.FileDownload)
    assert (
        template.source.url
        == "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/nyap_test.zip"
    )
