"""Possible refactor: abstract the by_race into a single function"""

from typing import List, Tuple

import pandas as pd
from aggregate.PUMS.aggregate_PUMS import PUMSCount
from aggregate.PUMS.economic_indicators import (
    industry_assign,
    lf_assign,
    occupation_assign,
)


class PUMSCountEconomics(PUMSCount):
    """Indicators refer to variables in Field Specifications page of data matrix"""

    indicators_denom: List[Tuple] = [
        ("lf",),
        (
            "occupation",
            "civilian_employed_pop_filter",
        ),  # Termed "Employment by occupation" in data matrix
        (
            "industry",
            "civilian_employed_pop_filter",
        ),  # Termed "Employment by industry sector" in data matrix
        ("education", "age_over_24_filter"),
    ]

    def __init__(
        self,
        limited_PUMA=False,
        year=2019,
        household=False,
        requery=False,
        add_MOE=True,
        keep_SE=False,
        geo_col: str = "puma",
    ) -> None:
        # self.crosstabs = ["race"]
        self.include_fractions = True
        self.include_counts = True
        self.categories = {}
        self.add_MOE = add_MOE
        self.keep_SE = keep_SE
        self.EDDT_category = "economics"
        if household:
            self.variable_types = ["households"]
            self.crosstabs = []
        else:
            self.variable_types = ["economics", "demographics"]
            self.crosstabs = ["race"]
        PUMSCount.__init__(
            self,
            variable_types=self.variable_types,
            limited_PUMA=limited_PUMA,
            year=year,
            requery=requery,
            geo_col=geo_col,
            household=household,
        )

    def lf_assign(self, person):
        return lf_assign(person)

    def occupation_assign(self, person):
        return occupation_assign(person)

    def industry_assign(self, person):
        return industry_assign(person)

    def education_assign(self, person):
        education = person["SCHL"]
        if education in [
            "Bachelor's degree",
            "Doctorate degree",
            "Master's degree",
            "Professional degree beyond a bachelor's degree",
        ]:
            return "Bachelors_or_higher"
        elif education in [
            "Some college, but less than 1 year",
            "Associate's degree",
            "1 or more years of college credit, no degree",
        ]:
            return "Some_college"
        elif education in [
            "Regular high school diploma",
            "GED or alternative credential",
        ]:
            return "high_school_or_equiv"

        return "less_than_hs_or_equiv"

    def age_over_24_filter(self, PUMS: pd.DataFrame):
        age_subset = PUMS[(PUMS["AGEP"] > 24)]
        return age_subset

    def civilian_employed_pop_filter(self, PUMS: pd.DataFrame):
        """Filter to return subset of all people ages 16-64 who are employed as civilians"""
        age_subset = PUMS[(PUMS["AGEP"] >= 16) & (PUMS["AGEP"] <= 64)]
        civilian_subset = age_subset[
            age_subset["ESR"].isin(
                [
                    "Civilian employed, with a job but not at work",
                    "Civilian employed, at work",
                ]
            )
        ]
        return civilian_subset
