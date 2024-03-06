from dcpy.connectors.edm import recipes
from dcpy.extract import utils


class TestSocrata:
    ## https://data.cityofnewyork.us/Social-Services/Outcome-of-Preventive-Cases-Closed-By-Borough-And-/q663-gvx6/about_data
    ## this dataset has "no plan for updates"
    ## it may eventually be deleted but for now is useful as a psuedo-constant
    test_set = recipes.ExtractConfig.Source.Socrata(
        type="socrata",
        org=recipes.ExtractConfig.Source.Socrata.Org.nyc,
        uid="q663-gvx6",
        format="csv",
    )

    def test_get_version(self):
        version = utils.Socrata.get_version(self.test_set)
        assert version == "20160606"
