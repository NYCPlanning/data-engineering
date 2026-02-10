"""Possible refactor: abstract the by_race into a single function"""

from typing import List, Tuple

import numpy as np
import pandas as pd
from aggregate.PUMS.aggregate_PUMS import PUMSCount


class PUMSCountHouseholds(PUMSCount):
    """Indicators refer to variables in Field Specifications page of data matrix"""

    indicators_denom: List[Tuple] = [
        ("household_income_bands", "household_type_filter"),
    ]

    def __init__(
        self,
        limited_PUMA=False,
        year=2019,
        requery=False,
        add_MOE=True,
        keep_SE=False,
        geo_col="puma",
    ) -> None:
        self.include_fractions = True
        self.include_counts = True
        self.categories = {}
        self.add_MOE = add_MOE
        self.keep_SE = keep_SE
        self.variable_types = ["households"]
        self.crosstabs = []
        self.household = True
        self.EDDT_category = "economics"
        PUMSCount.__init__(
            self,
            variable_types=self.variable_types,  # this is for the ingestion to pull correct weight and variables
            limited_PUMA=limited_PUMA,
            year=year,
            requery=requery,
            household=self.household,  # this is for aggregator to choose the right route for calculations
            geo_col=geo_col,
        )

    def household_income_bands_assign(self, person):
        income_bands = {
            1: [-9999999, 20900, 34835, 55735, 83602, 114952, 9999999],
            2: [-9999999, 23904, 39840, 63744, 95616, 131473, 9999999],
            3: [-9999999, 26876, 44794, 71671, 107506, 147821, 9999999],
            4: [-9999999, 29849, 49748, 79597, 119395, 164169, 9999999],
            5: [-9999999, 32258, 53763, 86021, 129032, 177419, 9999999],
            6: [-9999999, 34636, 57727, 92362, 138544, 190498, 99999999],
            7: [-9999999, 37014, 61690, 98703, 148055, 203576, 99999999],
            8: [-9999999, 39423, 65705, 105128, 157692, 216826, 99999999],
        }

        labels = ["ELI", "VLI", "LI", "MI", "MIDI", "HI"]

        if person["NPF"] > 8:
            idx = np.digitize(person["HINCP"], income_bands[8])
        else:
            idx = np.digitize(person["HINCP"], income_bands[person["NPF"]])

        return labels[idx - 1]

    def household_type_filter(self, PUMS: pd.DataFrame):
        """Filter to return subset of households only 1-7 in the HHT variable which is non-group quarter or vacant category"""

        non_gq_vac_subset = PUMS[(PUMS["HHT"] != "N/A (GQ/vacant)")]
        return non_gq_vac_subset
