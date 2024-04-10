"""Because indicators on multiple tabs in the data matrix crosstab by race, the code to crosswalk different race
columns to our final categories lives in the /aggregate folder. This code should be referenced by any indicator that crosstabs by race"""


def PUMS_race_assign(person):
    if person["HISP"] != "Not Spanish/Hispanic/Latino":
        return "hsp"
    else:
        if person["RAC1P"] == "White alone":
            return "wnh"
        elif person["RAC1P"] == "Black or African American alone":
            return "bnh"
        elif person["RAC1P"] == "Asian alone":
            return "anh"
        else:
            return "onh"


def HVS_race_assign(race):
    if race == 1:
        return "wnh"
    elif race == 2:
        return "bnh"
    elif race == 3 or race == 4:
        return "hsp"
    elif race == 4:
        return "anh"
    return "ohn"
