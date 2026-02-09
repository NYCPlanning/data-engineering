"""Each variable type (demographic, economic) maps to a certain set of variables.
However there are different set variables used in the GET url versus in the final dataset.
This module can handle each"""

from typing import List

from dotenv import load_dotenv

from dcpy.utils.logging import logger

load_dotenv()

variable_mapper = {
    "demographics": [
        ("RAC1P", "clean_simple_categorical"),
        ("HISP", "clean_simple_categorical"),
        ("NATIVITY", "clean_simple_categorical"),
        ("LANX", "clean_simple_categorical"),
        ("ENG", "clean_simple_categorical"),
        ("AGEP", "clean_continous"),
    ],
    "economics": [
        ("HINCP", "clean_continous"),  # Household income
        ("ESR", "clean_simple_categorical"),  # Employment status
        ("WAGP", "clean_continous"),  # Wages
        ("SCHL", "clean_simple_categorical"),  # Educational achievement
        ("INDP", "clean_range_categorical"),  # Industry
        ("OCCP", "occupation_clean_range_categorical"),  # Occupation
    ],
    "households": [
        ("HINCP", "clean_continous"),  # Household Income
        ("NPF", "clean_continous"),  # Number of Household Members
        ("HHT", "clean_simple_categorical"),  # Household Types
    ],
}


allowed_variable_types = ["demographics", "economics", "households"]


def variables_for_processing(variable_types: List, include_clean_method=True) -> List:
    rv = []
    for var_type in variable_types:
        if var_type not in allowed_variable_types:
            logger.error(f"{var_type} not one of {allowed_variable_types}")
        else:
            if include_clean_method:
                rv.extend(variable_mapper[var_type])
            else:
                rv.extend([x[0] for x in variable_mapper[var_type]])
    return rv


def variables_for_url(variable_types: List, year: int) -> str:
    """Sometimes variables in source data don't perfectly match what we want in
    output. In which case we have custom logic to deal with on a case by case basis"""
    variables = variables_for_processing(variable_types, include_clean_method=False)

    if "economics" in variable_types and year == 2012:
        variables.remove("OCCP")
        variables.extend(["OCCP02", "OCCP10", "OCCP12"])

    return ",".join(variables)
