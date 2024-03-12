import json
from pathlib import Path

from dcpy.connectors.edm.recipes import ExtractConfig
from dcpy.connectors.socrata import extract


RESOURCES = Path(__file__).parent / "resources"


def test_parse_version():
    with open(RESOURCES / "sample_api_response.json") as f:
        resp = json.load(f)
    version = extract._get_version_from_resp(resp)
    assert version == "20160606"


def test_get_version_from_socrata():
    ## https://data.cityofnewyork.us/Social-Services/Outcome-of-Preventive-Cases-Closed-By-Borough-And-/q663-gvx6/about_data
    ## this dataset has "no plan for updates"
    ## it may eventually be deleted but for now is useful as a psuedo-constant
    test_set = ExtractConfig.Source.Socrata(
        type="socrata",
        org=ExtractConfig.Source.Socrata.Org.nyc,
        uid="q663-gvx6",
        format="csv",
    )
    version = extract.get_version(test_set)
    assert version == "20160606"
