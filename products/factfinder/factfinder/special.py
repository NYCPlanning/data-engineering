import math

import numpy as np
import pandas as pd


def pivot(df: pd.DataFrame, base_variables: list) -> pd.DataFrame:
    dff = df.loc[:, ["census_geoid", "pff_variable", "e", "m"]].pivot(
        index="census_geoid", columns="pff_variable", values=["e", "m"]
    )
    pivoted = pd.DataFrame()
    pivoted["census_geoid"] = dff.index
    del df
    for i in base_variables:
        pivoted[i + "e"] = dff.e.loc[pivoted.census_geoid, i].to_list()
        pivoted[i + "m"] = dff.m.loc[pivoted.census_geoid, i].to_list()
    del dff
    return pivoted


def hovacrtm(hovacue, vacsalee, vacsalem, hovacum):
    if hovacue == 0:
        return 0
    elif vacsalee == 0:
        return 0
    elif vacsalem**2 - (vacsalee * hovacum / hovacue) ** 2 < 0:
        return (
            math.sqrt(vacsalem**2 + (vacsalee * hovacum / hovacue) ** 2) / hovacue * 100
        )
    else:
        return (
            math.sqrt(vacsalem**2 - (vacsalee * hovacum / hovacue) ** 2) / hovacue * 100
        )


def percapinc(df: pd.DataFrame, base_variables: list) -> pd.DataFrame:
    df = pivot(df, base_variables)
    df["e"] = df.agip15ple / df.pop_6e
    df["m"] = (
        1
        / df.pop_6e
        * np.sqrt(df.agip15plm**2 + (df.agip15ple * df.pop_6m / df.pop_6e) ** 2)
    )
    return df


def mntrvtm(df: pd.DataFrame, base_variables: list) -> pd.DataFrame:
    df = pivot(df, base_variables)
    df["e"] = df["agttme"] / (df["wrkr16ple"] - df["cw_wrkdhme"])
    df["m"] = (
        1
        / df["wrkrnothme"]
        * np.sqrt(
            df["agttmm"] ** 2
            + (df["agttme"] * df["wrkrnothmm"] / df["wrkrnothme"]) ** 2
        )
    )
    return df


def mnhhinc(df: pd.DataFrame, base_variables: list) -> pd.DataFrame:
    df = pivot(df, base_variables)
    df["e"] = df["aghhince"] / df["hh2e"]
    df["m"] = (
        1
        / df["hh5e"]
        * np.sqrt(df["aghhincm"] ** 2 + (df["aghhince"] * df["hh5m"] / df["hh5e"]) ** 2)
    )
    return df


def avghhsooc(df: pd.DataFrame, base_variables: list) -> pd.DataFrame:
    df = pivot(df, base_variables)
    df["e"] = df["popoochue"] / df["oochu1e"]
    df["m"] = (
        df["popoochum"] ** 2 + (df["popoochue"] * df["oochu4m"] / df["oochu4e"]) ** 2
    ) ** 0.5 / df["oochu4e"]
    return df


def avghhsroc(df: pd.DataFrame, base_variables: list) -> pd.DataFrame:
    df = pivot(df, base_variables)
    df["e"] = df["poprtochue"] / df["rochu1e"]
    df["m"] = (
        df["poprtochum"] ** 2 + (df["poprtochue"] * df["rochu2m"] / df["rochu2e"]) ** 2
    ) ** 0.5 / df["rochu2e"]
    return df


def avghhsz(df: pd.DataFrame, base_variables: list) -> pd.DataFrame:
    df = pivot(df, base_variables)
    df["e"] = df["hhpop1e"] / df["hh1e"]
    df["m"] = (
        df["hhpop1m"] ** 2 + (df["hh4m"] * df["hhpop1e"] / df["hh4e"]) ** 2
    ) ** 0.5 / df["hh4e"]
    return df


def avgfmsz(df: pd.DataFrame, base_variables: list) -> pd.DataFrame:
    df = pivot(df, base_variables)
    df["e"] = df["popinfmse"] / df["fam1e"]
    df["m"] = (
        df["popinfmsm"] ** 2 + (df["fam3m"] * df["popinfmse"] / df["fam3e"]) ** 2
    ) ** 0.5 / df["fam3e"]
    return df


def hovacrt(df: pd.DataFrame, base_variables: list) -> pd.DataFrame:
    df = pivot(df, base_variables)
    df["e"] = 100 * df["vacsalee"] / df["hovacue"]
    df["m"] = df.apply(
        lambda row: hovacrtm(
            row["hovacue"], row["vacsalee"], row["vacsalem"], row["hovacum"]
        ),
        axis=1,
    )
    df.loc[df.e == 0, "e"] = np.nan
    return df


def rntvacrt(df: pd.DataFrame, base_variables: list) -> pd.DataFrame:
    df = pivot(df, base_variables)
    df["e"] = 100 * df["vacrnte"] / df["rntvacue"]
    df["m"] = df.apply(
        lambda row: hovacrtm(
            row["rntvacue"], row["vacrnte"], row["vacrntm"], row["rntvacum"]
        ),
        axis=1,
    )
    df.loc[df.e == 0, "e"] = np.nan
    return df


def wrkrnothm(df: pd.DataFrame, base_variables: list) -> pd.DataFrame:
    df = pivot(df, base_variables)
    df["e"] = df["wrkr16ple"] - df["cw_wrkdhme"]
    df["m"] = (df["wrkr16plm"] ** 2 + df["cw_wrkdhmm"] ** 2) ** 0.5
    return df
