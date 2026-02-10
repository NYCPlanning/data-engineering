"""indicators_denom attr is defined first as constant outside of class so that it
can be imported on it's own"""

import pandas as pd
from aggregate.aggregation_helpers import demographic_indicators_denom
from aggregate.PUMS.aggregate_PUMS import PUMSCount


class PUMSCountDemographics(PUMSCount):
    """Medians aggregator has crosstabs in data structure instead of appended as text. This may be better design
    Indicators are being extended multiple times, not good
    To-do: break out calculations into __call__ that can get counts, fractions, or both
    """

    cache_fn = "data/PUMS_demographic_counts_aggregator.pkl"  # Can make this dynamic based on position on inheritance tree
    indicators_denom = demographic_indicators_denom

    def __init__(
        self,
        year: int,
        limited_PUMA=False,
        requery=False,
        include_counts=True,
        include_fractions=True,
        add_MOE=True,
        keep_SE=False,
        single_indicator=False,
        geo_col="puma",
    ) -> None:
        print(
            f"top of pums count demographics init. indicators denom is {self.indicators_denom}"
        )

        if single_indicator:
            self.indicators_denom = self.indicators_denom[0:1]
        # self.indicators_denom = list(
        #     set(self.indicators_denom)
        # )  # To-do: figure out problem and undo hot fix
        self.crosstabs = ["race"]
        self.categories = {}
        self.include_counts = include_counts
        self.include_fractions = include_fractions
        self.add_MOE = add_MOE
        self.keep_SE = keep_SE
        self.EDDT_category = "demographics"
        PUMSCount.__init__(
            self,
            year=year,
            variable_types=["demographics"],
            limited_PUMA=limited_PUMA,
            requery=requery,
            geo_col=geo_col,
            household=False,
        )

    def foreign_born_assign(self, person):
        """Foreign born"""
        if person["NATIVITY"] != "Native":
            return "fb"
        return None

    def LEP_assign(self, person):
        """Limited english proficiency"""
        if person["ENG"] in ["Not at all", "Not well", "Well"]:
            return "lep"
        return None

    def age_bucket_assign(self, person):
        if person["AGEP"] < 16:
            return "age_popu16"
        if person["AGEP"] >= 16 and person["AGEP"] < 65:
            return "age_p16t64"
        if person["AGEP"] >= 65:
            return "age_p65pl"

    def over_five_filter(self, PUMS: pd.DataFrame):
        subset = PUMS[PUMS["AGEP"] >= 5]
        return subset
