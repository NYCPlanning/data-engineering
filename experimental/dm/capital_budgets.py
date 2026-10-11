# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "altair",
#     "duckdb",
#     "marimo",
#     "obstore",
#     "pandas",
#     "pyarrow",
# ]
# ///
import marimo

__generated_with = "0.25.1"
app = marimo.App(width="medium")

with app.setup(hide_code=True):
    import io
    import os
    from pathlib import Path

    import altair as alt
    import duckdb
    import marimo as mo
    import obstore as obs
    import pandas as pd
    from obstore.store import S3Store

    REPO_DIR = Path(__file__).resolve().parents[2]
    SEEDS_DIR = REPO_DIR / "products" / "cpdb" / "seeds"
    TYCS_STAGING_SQL = (
        REPO_DIR / "products" / "cpdb" / "models" / "staging" / "stg__omb_tycs.sql"
    )


@app.cell(hide_code=True)
def _():
    mo.md("""
    # Capital budgets

    Ten-Year Capital Strategy (TYCS) releases, Capital Commitment Plans (CCP), and spending, across releases.

    - how capital planning goes from budgets to projects
    - how TYCS categories line up with CCP categories
    - how planned amounts line up between each TYCS and its CCP
    """)
    return


@app.cell(hide_code=True)
def _():
    recipes_store = S3Store(
        os.environ["RECIPES_BUCKET"],
        endpoint=os.environ["AWS_S3_ENDPOINT"],
        access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region="us-east-1",
    )
    return (recipes_store,)


@app.cell(hide_code=True)
def _(recipes_store):
    def read_recipe(dataset: str, version: str) -> pd.DataFrame:
        """Read an archived dataset version from edm-recipes as all-string columns."""
        folder = f"datasets/{dataset}/{version}/"
        paths = {m["path"] for m in obs.list(recipes_store, folder).collect()}
        stem = f"{folder}{dataset}"
        if f"{stem}.parquet" in paths:
            data = obs.get(recipes_store, f"{stem}.parquet").bytes()
            return pd.read_parquet(io.BytesIO(bytes(data))).astype("string")
        if f"{stem}.csv" in paths:
            data = obs.get(recipes_store, f"{stem}.csv").bytes()
            return pd.read_csv(io.BytesIO(bytes(data)), dtype="string")
        # older versions were only archived as plain-format pg_dumps: read the COPY block
        lines = (
            bytes(obs.get(recipes_store, f"{stem}.sql").bytes()).decode().splitlines()
        )
        start = next(i for i, line in enumerate(lines) if line.startswith("COPY "))
        header = lines[start]
        columns = [
            c.strip().strip('"')
            for c in header[header.index("(") + 1 : header.index(")")].split(",")
        ]
        rows = [
            line.split("\t") for line in lines[start + 1 : lines.index("\\.", start)]
        ]
        return pd.DataFrame(rows, columns=columns, dtype="string").replace("\\N", pd.NA)

    return (read_recipe,)


@app.cell
def _():
    def whole_numbers(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        """Round amounts (thousands of dollars) and counts to whole numbers for display."""
        return df.assign(**{c: df[c].round().astype("Int64") for c in columns})

    # clearer names for displayed tables; cells keep the shorter names in code
    display_names = {
        "paid": "paid ($K)",
        "planned": "planned ($K)",
        "contracts": "contracts_paid",
        "pub_date": "release_date",
        "cycle_fy": "fisa_cycle_fy",
        "cycle_name": "fisa_cycle_name",
        "deliveries": "fisa_deliveries",
        "yr1_fy": "first_published_fy",
        "num_yrs_prsnt": "published_years",
    }

    def for_display(df: pd.DataFrame, hide: tuple[str, ...] = ()) -> pd.DataFrame:
        """Drop helper columns and rename the rest for display."""
        return df.drop(columns=list(hide)).rename(columns=display_names)

    return for_display, whole_numbers


@app.cell(hide_code=True)
def _(read_recipe):
    # TYCS is realigned with the cpdb staging model's own SQL, run in duckdb instead of Postgres
    tycs_db = duckdb.connect()
    tycs_db.register("tycs_raw", read_recipe("omb_tycs", "latest"))
    tycs = tycs_db.sql(
        TYCS_STAGING_SQL.read_text()
        .replace("{{ source('recipe_sources', 'omb_tycs') }}", "tycs_raw")
        .replace(
            "to_date(pub_date, 'YYYYMMDD')",
            "strptime(pub_date, '%Y%m%d')::date",
        )
    ).df()
    fisa_typc_names = pd.read_csv(SEEDS_DIR / "fisa_typc_names.csv", dtype="string")
    tycs_manual_matches = pd.read_csv(
        SEEDS_DIR / "tycs_category_matches.csv", dtype="string"
    )
    return fisa_typc_names, tycs, tycs_manual_matches


@app.cell
def _(fisa_typc_names, tycs, tycs_manual_matches):
    # a CCP code matches the TYCS category with any name the code has had in FISA, or by
    # hand, the same way as cpdb's int__ten_year_plan_category_tycs_matches
    tycs_names = (
        tycs.assign(category=lambda d: d["typc"].str.upper())
        .groupby(["projtype", "category"], as_index=False)
        .agg(tycs_category=("typc", "first"), last_pub_date=("pub_date", "max"))
    )
    name_matches = (
        fisa_typc_names.assign(
            project_type=lambda d: d["budget_proj_type"],
            projtype=lambda d: d["budget_proj_type"].replace(
                {"BR": "BR and HB", "HB": "BR and HB"}
            ),
            category=lambda d: d["typc_name"].str.upper(),
        )[["project_type", "typc", "projtype", "category"]]
        .drop_duplicates()
        .merge(tycs_names, on=["projtype", "category"])
        .sort_values("last_pub_date")
        .drop_duplicates(["project_type", "typc"], keep="last")
        .rename(columns={"projtype": "tycs_project_type"})
    )
    code_tycs_matches = pd.concat(
        [
            tycs_manual_matches,
            name_matches.merge(
                tycs_manual_matches[["project_type", "typc"]],
                on=["project_type", "typc"],
                how="left",
                indicator=True,
            ).query("_merge == 'left_only'"),
        ]
    )[["project_type", "typc", "tycs_project_type", "tycs_category"]]
    return (code_tycs_matches,)


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Pairing TYCS releases with CCPs

    A CCP in FISA is identified by `cycle_fy` (the fiscal year it was published in) and
    `cycle_name`. A preliminary TYCS comes out in January with the preliminary CCP, and a
    final TYCS in April or May with the executive CCP.
    """)
    return


@app.cell(hide_code=True)
def _(fisa_typc_names, for_display, tycs):
    tycs_releases = pd.DataFrame(
        {"pub_date": pd.to_datetime(sorted(tycs["pub_date"].unique()))}
    ).assign(
        edition=lambda d: d.pub_date.dt.month.map(
            lambda m: "preliminary" if m < 3 else "final"
        ),
        cycle_fy=lambda d: (d.pub_date.dt.year + (d.pub_date.dt.month >= 7)).astype(
            "string"
        ),
        cycle_name=lambda d: d.edition.map(
            {"preliminary": "CPJ FP", "final": "CPA FP"}
        ),
    )
    # a cycle can have more than one delivery (2026 CPJ and CPA each have two); use the last
    fisa_versions = (
        fisa_typc_names[["version", "cycle_fy", "cycle_name"]]
        .drop_duplicates()
        .groupby(["cycle_fy", "cycle_name"], as_index=False)
        .agg(fisa_version=("version", "max"), deliveries=("version", "count"))
    )
    tycs_fisa_pairs = tycs_releases.merge(
        fisa_versions, on=["cycle_fy", "cycle_name"], how="left"
    )
    # deliveries is missing for the TYCS releases with no archived CCP
    tycs_fisa_pairs = tycs_fisa_pairs.astype({"deliveries": "Int64"})
    for_display(tycs_fisa_pairs)
    return (tycs_fisa_pairs,)


@app.cell
def _(fisa_typc_names, for_display):
    # OMB release date and published fiscal years of every Capital Commitment Plan, from the
    # Open Data version of the CCP (2cmn-uidm), matched to FISA cycles by release month
    ccp_open_data = pd.read_json(
        "https://data.cityofnewyork.us/resource/2cmn-uidm.json"
        "?$select=pub_date,yr1_fy,num_yrs_prsnt&$group=pub_date,yr1_fy,num_yrs_prsnt",
        dtype={"pub_date": str},
    ).assign(pub_date=lambda d: pd.to_datetime(d["pub_date"].str[:8], format="%Y%m%d"))
    # release months for each FISA cycle: January to March, April to June, September to December
    cycle_months = {"CPJ FP": (1, 3), "CPA FP": (4, 6), "CPS FP": (9, 12)}
    ccp_releases = (
        fisa_typc_names[["version", "cycle_fy", "cycle_name"]]
        .drop_duplicates()
        .groupby(["cycle_fy", "cycle_name"], as_index=False)
        .agg(fisa_version=("version", "max"))
        .assign(
            is_adopted=lambda d: (d["cycle_name"] == "CPS FP").astype(int),
            # a September CCP is released in the fiscal year it funds, a calendar year earlier
            release_year=lambda d: d["cycle_fy"].astype(int) - d["is_adopted"],
            budget_fy=lambda d: d["cycle_fy"].astype(int) + 1 - d["is_adopted"],
            edition=lambda d: d["cycle_name"].map(
                {"CPJ FP": "prelim", "CPA FP": "exec", "CPS FP": "adopted"}
            ),
            first_month=lambda d: d["cycle_name"].map(lambda c: cycle_months[c][0]),
            last_month=lambda d: d["cycle_name"].map(lambda c: cycle_months[c][1]),
        )
        .merge(ccp_open_data, how="cross")
        .loc[
            lambda d: (
                (d["pub_date"].dt.year == d["release_year"])
                & d["pub_date"].dt.month.between(d["first_month"], d["last_month"])
            )
        ]
        .drop(columns=["is_adopted", "first_month", "last_month"])
        .reset_index(drop=True)
    )
    ccp_releases = ccp_releases.astype({"yr1_fy": int})
    for_display(ccp_releases, hide=("release_year",))
    return (ccp_releases,)


@app.cell
def _(ccp_releases, plan_funding, read_recipe):
    # first and last fiscal year with planned money in every archived FISA CCP, for the
    # timeline (CCPs also carry zero-amount rows out to later years); reading all of them takes
    # a few minutes, so the result is cached to disk
    with mo.persistent_cache(name="fisa_extents"):
        fisa_extents = (
            pd.concat(
                read_recipe("fisa_capitalcommitments", version)[
                    ["planned_commit_date", *plan_funding]
                ].assign(fisa_version=version)
                for version in ccp_releases["fisa_version"]
            )
            .loc[lambda d: d[list(plan_funding)].astype(float).ne(0).any(axis=1)]
            .assign(
                planned_fy=lambda d: (
                    d["planned_commit_date"].str[:4].astype(int)
                    + (d["planned_commit_date"].str[4:6].astype(int) >= 7)
                )
            )
            .groupby("fisa_version")["planned_fy"]
            .agg(["min", "max"])
        )
    return (fisa_extents,)


@app.cell
def _(ccp_releases, plan_funding, read_recipe):
    # planned commitments by project in every archived CCP, for project histories; reading all
    # of them takes a few minutes, so the result is cached to disk
    with mo.persistent_cache(name="all_ccp_commitments"):
        all_ccp_commitments = (
            pd.concat(
                read_recipe("fisa_capitalcommitments", version)[
                    [
                        "managing_agcy_cd",
                        "project_id",
                        "object",
                        "planned_commit_date",
                        *plan_funding,
                    ]
                ].assign(fisa_version=version)
                for version in ccp_releases["fisa_version"]
            )
            .apply(lambda s: s.str.strip())
            .assign(
                maprojid=lambda d: d["managing_agcy_cd"].str.zfill(3) + d["project_id"],
                amount=lambda d: d[list(plan_funding)].astype(float).sum(axis=1) / 1000,
            )
            .loc[lambda d: d["amount"] != 0]
            .groupby(
                ["fisa_version", "maprojid", "planned_commit_date", "object"],
                as_index=False,
            )["amount"]
            .sum()
        )
    return (all_ccp_commitments,)


@app.cell(hide_code=True)
def _(read_recipe, tycs_fisa_pairs):
    # planned commitments in long form: one row per FISA row and funding source, in thousands
    # like the TYCS. City funding is split into non-exempt (cnx) and exempt (cex) in FISA.
    plan_funding = {
        "fcst_cnx_amt": "City",
        "fcst_cex_amt": "City",
        "fcst_st_amt": "State",
        "fcst_fd_amt": "Federal",
        "fcst_pv_amt": "Private",
    }
    plan_commitments = (
        pd.concat(
            read_recipe("fisa_capitalcommitments", version).assign(fisa_version=version)
            for version in tycs_fisa_pairs["fisa_version"].dropna()
        )
        .apply(lambda s: s.str.strip())
        .melt(
            id_vars=[
                "fisa_version",
                "budget_proj_type",
                "budget_line_id",
                "managing_agcy_cd",
                "project_id",
                "short_descr",
                "object",
                "planned_commit_date",
                "typ_category",
                "typ_category_name",
            ],
            value_vars=list(plan_funding),
            var_name="amount_column",
            value_name="amount",
        )
        .assign(
            # some archived versions dropped the leading zero from two-digit agency codes
            # (57 for 057), which Checkbook and newer versions keep
            managing_agcy_cd=lambda d: d["managing_agcy_cd"].str.zfill(3),
            maprojid=lambda d: d["managing_agcy_cd"] + d["project_id"],
            funding=lambda d: d["amount_column"].map(plan_funding),
            amount=lambda d: d["amount"].astype(float) / 1000,
            planned_fy=lambda d: (
                d["planned_commit_date"].str[:4].astype(int)
                + (d["planned_commit_date"].str[4:].astype(int) >= 7)
            ),
        )
        .query("amount != 0")
        .drop(columns="amount_column")
    )
    return plan_commitments, plan_funding


@app.cell(hide_code=True)
def _(ccp_releases, fisa_extents, tycs_fisa_pairs):
    # one row per release: a dot at the release date, bars over the fiscal years it covers
    # (fiscal year N runs July 1 of N-1 through June 30 of N). OMB titles a CCP by the budget
    # it goes with. The FISA extract runs further than the published years.
    def fy_start(fy):
        return pd.Timestamp(year=int(fy) - 1, month=7, day=1)

    def fy_end(fy):
        return pd.Timestamp(year=int(fy), month=6, day=30)

    def fy_label(fy):
        return f"FY{int(fy) % 100:02d}"

    tycs_rows = tycs_fisa_pairs.assign(
        release=lambda d: [
            f"TYCS {fy_label(p.year + 1)} {'prelim' if e == 'preliminary' else e} ({p:%Y-%m-%d})"
            for p, e in zip(d["pub_date"], d["edition"])
        ],
        segment="TYCS",
        start=lambda d: [fy_start(p.year + 1) for p in d["pub_date"]],
        end=lambda d: [fy_end(p.year + 10) for p in d["pub_date"]],
    )
    ccp_rows = ccp_releases.assign(
        release=lambda d: [
            f"CCP {fy_label(f)} {e} ({p:%Y-%m-%d})"
            for f, e, p in zip(d["budget_fy"], d["edition"], d["pub_date"])
        ]
    )
    release_timeline = pd.concat(
        [
            tycs_rows,
            ccp_rows.assign(
                segment="CCP published",
                start=lambda d: [fy_start(f) for f in d["yr1_fy"]],
                end=lambda d: [
                    fy_end(f + n - 1) for f, n in zip(d["yr1_fy"], d["num_yrs_prsnt"])
                ],
            ),
            ccp_rows.assign(
                segment="CCP FISA extract only",
                start=lambda d: [
                    fy_start(f + n) for f, n in zip(d["yr1_fy"], d["num_yrs_prsnt"])
                ],
                end=lambda d: [
                    fy_end(fisa_extents.loc[v, "max"]) for v in d["fisa_version"]
                ],
            ),
        ]
    )[["release", "segment", "pub_date", "start", "end"]].sort_values(
        ["pub_date", "segment"], ascending=[True, False]
    )
    release_order = list(dict.fromkeys(release_timeline["release"]))
    timeline_bars = (
        alt.Chart(release_timeline)
        .mark_bar(height=8)
        .encode(
            x=alt.X("start:T", title=None, axis=alt.Axis(format="%Y")),
            x2="end:T",
            y=alt.Y("release:N", sort=release_order, title=None),
            color=alt.Color(
                "segment:N",
                title="fiscal years covered",
                scale=alt.Scale(
                    domain=["TYCS", "CCP published", "CCP FISA extract only"],
                    range=["#4c78a8", "#f58518", "#fdd0a2"],
                ),
            ),
            tooltip=[
                "release",
                "segment",
                alt.Tooltip("start:T", title="from"),
                alt.Tooltip("end:T", title="through"),
            ],
        )
    )
    timeline_dots = (
        alt.Chart(
            release_timeline.drop_duplicates("release").assign(marker="release date")
        )
        .mark_point(filled=True, color="black")
        .encode(
            x="pub_date:T",
            y=alt.Y("release:N", sort=release_order),
            shape=alt.Shape("marker:N", title=None, scale=alt.Scale(range=["circle"])),
            tooltip=["release"],
        )
    )
    (timeline_bars + timeline_dots).properties(
        width=650,
        title="TYCS and CCP releases and the fiscal years they cover",
    )
    return ccp_rows, tycs_rows


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Categories: TYCS vs CCP

    The TYCS names categories, and the CCP has a code and a name for each. A CCP code
    matches the TYCS category with any name the code has had in FISA, ignoring case, or by
    hand through the `tycs_category_matches` seed. The TYCS reports waterway and highway
    bridges together as `BR and HB`, so the CCP's BR and HB are combined to match.
    """)
    return


@app.cell(hide_code=True)
def _(code_tycs_matches, plan_commitments, tycs, tycs_fisa_pairs):
    tycs_categories = (
        tycs.merge(
            tycs_fisa_pairs[["pub_date", "fisa_version"]].dropna(),
            on="pub_date",
        )
        .assign(category=lambda d: d["typc"].str.upper())
        .groupby(["fisa_version", "projtype", "category"], as_index=False)
        .agg(tycs_name=("typc", "first"), tycs_total=("total", "sum"))
    )
    # CCP categories take their TYCS project type and name from the code's TYCS match,
    # which covers renamed categories and the combined BR and HB program
    plan_commitments_tycs = plan_commitments.merge(
        code_tycs_matches,
        left_on=["budget_proj_type", "typ_category"],
        right_on=["project_type", "typc"],
        how="left",
    ).assign(
        projtype=lambda d: d["tycs_project_type"].fillna(
            d["budget_proj_type"].replace({"BR": "BR and HB", "HB": "BR and HB"})
        ),
        category=lambda d: (
            d["tycs_category"].fillna(d["typ_category_name"]).str.upper()
        ),
    )
    plan_categories = plan_commitments_tycs.groupby(
        ["fisa_version", "projtype", "category"], as_index=False
    ).agg(
        typc=("typ_category", lambda s: ", ".join(sorted(s.unique()))),
        plan_total=("amount", "sum"),
    )
    category_matches = tycs_categories.merge(
        plan_categories,
        on=["fisa_version", "projtype", "category"],
        how="outer",
        indicator="found_in",
    ).assign(
        found_in=lambda d: d["found_in"].map(
            {
                "both": "both",
                "left_only": "TYCS only",
                "right_only": "CCP only",
            }
        )
    )
    return category_matches, plan_commitments_tycs


@app.cell(hide_code=True)
def _(category_matches, tycs_fisa_pairs, whole_numbers):
    category_match_summary = (
        category_matches.groupby(["fisa_version", "found_in"])
        .agg(
            categories=("category", "size"),
            tycs_total=("tycs_total", "sum"),
            plan_total=("plan_total", "sum"),
        )
        .reset_index()
    )
    category_match_summary = whole_numbers(
        category_match_summary, ["tycs_total", "plan_total"]
    )
    alt.Chart(category_match_summary).mark_bar().encode(
        x=alt.X("categories:Q", stack="normalize", title="share of categories"),
        y=alt.Y(
            "fisa_version:N",
            sort=list(tycs_fisa_pairs["fisa_version"].dropna()),
            title="CCP (FISA version)",
        ),
        color=alt.Color("found_in:N", title="found in"),
        tooltip=[
            "fisa_version",
            "found_in",
            "categories",
            alt.Tooltip("plan_total:Q", format=",.0f"),
            alt.Tooltip("tycs_total:Q", format=",.0f"),
        ],
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Planned amounts by fiscal year

    Each TYCS amount is compared with its paired CCP's planned commitments for the same
    fiscal year and category, all funding sources. The CCP also covers the fiscal year it was
    released in, which the TYCS doesn't, so that year is left out.
    """)
    return


@app.cell(hide_code=True)
def _(plan_commitments_tycs, tycs, tycs_fisa_pairs):
    tycs_by_fy = (
        tycs.merge(
            tycs_fisa_pairs[["pub_date", "fisa_version"]].dropna(),
            on="pub_date",
        )
        .melt(
            id_vars=["fisa_version", "projtype", "typc", "funding", "yr1_fy"],
            value_vars=[f"year{i}_amt" for i in range(1, 11)],
            var_name="year",
            value_name="tycs_amount",
        )
        .assign(
            fy=lambda d: (
                d["yr1_fy"]
                + d["year"].str.extract(r"(\d+)", expand=False).astype(int)
                - 1
            ),
            category=lambda d: d["typc"].str.upper(),
        )
        .groupby(["fisa_version", "projtype", "category", "fy"], as_index=False)[
            "tycs_amount"
        ]
        .sum()
    )
    plan_by_fy = (
        plan_commitments_tycs.rename(columns={"planned_fy": "fy"})
        .groupby(["fisa_version", "projtype", "category", "fy"], as_index=False)
        .agg(plan_amount=("amount", "sum"))
    )
    amounts_by_fy = tycs_by_fy.merge(
        plan_by_fy,
        on=["fisa_version", "projtype", "category", "fy"],
        how="left",
    ).fillna({"plan_amount": 0})
    fy_totals = (
        amounts_by_fy.groupby(["fisa_version", "fy"], as_index=False)[
            ["tycs_amount", "plan_amount"]
        ]
        .sum()
        .assign(pct_diff=lambda d: d["plan_amount"] / d["tycs_amount"] - 1)
    )
    return amounts_by_fy, fy_totals


@app.cell(hide_code=True)
def _(fy_totals, tycs_fisa_pairs):
    pair_order = list(tycs_fisa_pairs["fisa_version"].dropna())
    tolerance = (
        alt.Chart(pd.DataFrame({"low": [-0.02], "high": [0.02]}))
        .mark_rect(opacity=0.15)
        .encode(y="low:Q", y2="high:Q")
    )
    fy_lines = (
        alt.Chart(fy_totals)
        .mark_line(point=True)
        .encode(
            x=alt.X("fy:O", title="fiscal year"),
            y=alt.Y("pct_diff:Q", axis=alt.Axis(format="%"), title="CCP vs TYCS"),
            color=alt.Color(
                "fisa_version:N", sort=pair_order, title="CCP (FISA version)"
            ),
            tooltip=[
                "fisa_version",
                "fy",
                alt.Tooltip("tycs_amount:Q", format=",.0f", title="TYCS ($K)"),
                alt.Tooltip("plan_amount:Q", format=",.0f", title="CCP ($K)"),
                alt.Tooltip("pct_diff:Q", format=".1%"),
            ],
        )
    )
    (tolerance + fy_lines).properties(
        width=600, title="CCP vs TYCS by fiscal year (shaded: ±2%)"
    )
    return (pair_order,)


@app.cell(hide_code=True)
def _(amounts_by_fy, fy_totals, pair_order, plan_commitments_tycs):
    # each CCP row in a TYCS category goes in one bucket: contingency commitments (FISA object
    # CTGY), budget lines that are usually City Council (D) or Borough President (borough
    # letter) money, or everything else. The TYCS is then compared with the CCP as each
    # bucket is taken out.
    line_types = {
        "D": "Council or BP",
        **{b: "Council or BP" for b in "MXKQR"},
    }
    plan_by_bucket = (
        plan_commitments_tycs.merge(
            amounts_by_fy[["fisa_version", "projtype", "category"]].drop_duplicates(),
            on=["fisa_version", "projtype", "category"],
        )
        .assign(
            fy=lambda d: d["planned_fy"],
            bucket=lambda d: (
                d["budget_line_id"]
                .str[0]
                .map(line_types)
                .fillna("other")
                .where(d["object"] != "CTGY", "contingency")
            ),
        )
        .pivot_table(
            index=["fisa_version", "fy"],
            columns="bucket",
            values="amount",
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
    )
    gap_by_bucket = (
        fy_totals.merge(plan_by_bucket, on=["fisa_version", "fy"])
        .assign(
            all_ccp=lambda d: d["pct_diff"],
            without_contingency=lambda d: (
                (d["plan_amount"] - d["contingency"]) / d["tycs_amount"] - 1
            ),
            without_contingency_council_bp=lambda d: d["other"] / d["tycs_amount"] - 1,
        )
        .melt(
            id_vars=["fisa_version", "fy"],
            value_vars=[
                "all_ccp",
                "without_contingency",
                "without_contingency_council_bp",
            ],
            var_name="plan_rows",
            value_name="diff",
        )
    )
    alt.Chart(gap_by_bucket).mark_line(point=True).encode(
        x=alt.X("fy:O", title="fiscal year"),
        y=alt.Y("diff:Q", axis=alt.Axis(format="%"), title="CCP vs TYCS"),
        color=alt.Color(
            "plan_rows:N",
            title="CCP rows",
            sort=[
                "all_ccp",
                "without_contingency",
                "without_contingency_council_bp",
            ],
        ),
        facet=alt.Facet("fisa_version:N", sort=pair_order, columns=3, title=None),
        tooltip=[
            "fisa_version",
            "fy",
            "plan_rows",
            alt.Tooltip("diff:Q", format=".1%"),
        ],
    ).properties(width=220, height=160)
    return


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## From budgets to projects to payments

    The TYCS sets amounts by project type and category. The CCP breaks a category into budget
    lines (what the money is appropriated to) and projects (FMS IDs, `maprojid`), each with
    planned commitments by month and commitment code. Checkbook NYC records the payments, which
    join to projects on the first 12 characters of `capital_project`.
    """)
    return


@app.cell
def _(recipes_store):
    # Checkbook NYC capital payments by project, contract, and month, in thousands like the CCP;
    # cached since the archive is large
    with mo.persistent_cache(name="checkbook_payments"):
        checkbook_payments = (
            pd.read_parquet(
                io.BytesIO(
                    bytes(
                        obs.get(
                            recipes_store,
                            "datasets/nycoc_checkbook/latest/nycoc_checkbook.parquet",
                        ).bytes()
                    )
                ),
                columns=[
                    "capital_project",
                    "contract_id",
                    "payee_name",
                    "expense_category",
                    "fiscal_year",
                    "issue_date",
                    "check_amount",
                ],
            )
            .assign(
                maprojid=lambda d: d["capital_project"].str[:12].str.strip(),
                paid_month=lambda d: d["issue_date"].str[:7],
                fiscal_year=lambda d: d["fiscal_year"].astype(int),
                amount=lambda d: d["check_amount"].astype(float) / 1000,
            )
            .groupby(
                [
                    "maprojid",
                    "contract_id",
                    "payee_name",
                    "expense_category",
                    "fiscal_year",
                    "paid_month",
                ],
                as_index=False,
                dropna=False,
            )["amount"]
            .sum()
        )
    return (checkbook_payments,)


@app.cell
def _(tycs_fisa_pairs):
    plan_picker = mo.ui.dropdown(
        options={
            f"{v} ({e})": v
            for v, e in zip(tycs_fisa_pairs["fisa_version"], tycs_fisa_pairs["edition"])
            if pd.notna(v)
        },
        value="20250503 (final)",
        label="TYCS and CCP pair",
    )
    plan_picker
    return (plan_picker,)


@app.cell
def _(amounts_by_fy, plan_picker):
    pair_categories = amounts_by_fy.loc[
        amounts_by_fy["fisa_version"] == plan_picker.value
    ]
    category_picker = mo.ui.dropdown(
        options=sorted(
            f"{p}: {c}"
            for p, c in pair_categories[["projtype", "category"]]
            .drop_duplicates()
            .itertuples(index=False)
        ),
        value="WP: PLANT UPGRADING AND RECONSTRUCTION",
        label="TYCS category",
        searchable=True,
    )
    category_picker
    return category_picker, pair_categories


@app.cell
def _(
    category_picker,
    checkbook_payments,
    for_display,
    pair_categories,
    plan_commitments_tycs,
    plan_picker,
    whole_numbers,
):
    selected_projtype, selected_category = category_picker.value.split(": ", 1)
    selected_rows = plan_commitments_tycs.loc[
        (plan_commitments_tycs["fisa_version"] == plan_picker.value)
        & (plan_commitments_tycs["projtype"] == selected_projtype)
        & (plan_commitments_tycs["category"] == selected_category)
    ]
    selected_payments = checkbook_payments.loc[
        checkbook_payments["maprojid"].isin(selected_rows["maprojid"])
    ]
    # wording agencies use in project descriptions for lump sums and placeholders, found by
    # searching every paired CCP; it varies by agency, so the matched word is shown, not a flag
    lump_sum_keywords = (
        r"(?i)(agency proposed|lump sum|outyear|future proj|holding co?de)"
    )
    category_projects = (
        selected_rows.groupby("maprojid", as_index=False)
        .agg(
            description=("short_descr", "first"),
            first_planned=("planned_commit_date", "min"),
            last_planned=("planned_commit_date", "max"),
            planned=("amount", "sum"),
            budget_lines=(
                "budget_line_id",
                lambda s: ", ".join(sorted(s.unique())),
            ),
            commitment_codes=(
                "object",
                lambda s: ", ".join(sorted(s.unique())),
            ),
            has_only_othr_commitments=("object", lambda s: set(s) == {"OTHR"}),
        )
        .assign(
            first_planned=lambda d: (
                d["first_planned"].str[:4] + "-" + d["first_planned"].str[4:]
            ),
            last_planned=lambda d: (
                d["last_planned"].str[:4] + "-" + d["last_planned"].str[4:]
            ),
        )
        .merge(
            selected_payments.groupby("maprojid", as_index=False).agg(
                first_paid=("paid_month", "min"),
                last_paid=("paid_month", "max"),
                contracts=("contract_id", "nunique"),
                paid=("amount", "sum"),
            ),
            on="maprojid",
            how="left",
        )
        .fillna({"contracts": 0, "paid": 0})
        .assign(
            description_keyword=lambda d: (
                d["description"]
                .str.extract(lump_sum_keywords, expand=False)
                .str.lower()
                .str.replace("holding cde", "holding code")
                .str.title()
            ),
            # chronological by the first thing that happened: a payment or a planned commitment
            first_activity=lambda d: d[["first_paid", "first_planned"]].min(axis=1),
        )
        .sort_values(["first_activity", "maprojid"])[
            [
                "maprojid",
                "description",
                "first_paid",
                "last_paid",
                "paid",
                "contracts",
                "first_planned",
                "last_planned",
                "planned",
                "budget_lines",
                "commitment_codes",
                "has_only_othr_commitments",
                "description_keyword",
            ]
        ]
    )
    category_funnel = pd.DataFrame(
        {
            "level": [
                "TYCS category, 10 years",
                "budget lines",
                "projects",
                "planned commitments, all CCP years",
                "projects with payments",
                "contracts paid, FY2010 on, all categories",
            ],
            "count": [
                1,
                selected_rows["budget_line_id"].nunique(),
                selected_rows["maprojid"].nunique(),
                len(selected_rows),
                int((category_projects["paid"] != 0).sum()),
                selected_payments["contract_id"].nunique(),
            ],
            "amount ($K)": [
                pair_categories.loc[
                    (pair_categories["projtype"] == selected_projtype)
                    & (pair_categories["category"] == selected_category),
                    "tycs_amount",
                ].sum(),
                None,
                None,
                selected_rows["amount"].sum(),
                None,
                selected_payments["amount"].sum(),
            ],
        }
    )
    category_payments = (
        selected_payments.groupby(
            ["maprojid", "contract_id", "payee_name", "expense_category"],
            as_index=False,
            dropna=False,
        )
        .agg(
            first_paid=("paid_month", "min"),
            last_paid=("paid_month", "max"),
            paid=("amount", "sum"),
        )
        .merge(category_projects[["maprojid", "description"]], on="maprojid")
        .sort_values(["first_paid", "last_paid", "maprojid"])[
            [
                "first_paid",
                "last_paid",
                "maprojid",
                "description",
                "contract_id",
                "payee_name",
                "expense_category",
                "paid",
            ]
        ]
    )
    category_funnel = whole_numbers(category_funnel, ["amount ($K)"])
    category_projects = whole_numbers(
        category_projects, ["planned", "contracts", "paid"]
    ).reset_index(drop=True)
    category_payments = whole_numbers(category_payments, ["paid"]).reset_index(
        drop=True
    )
    mo.vstack(
        [
            for_display(category_funnel),
            mo.md("**Projects** in the category's planned commitments"),
            for_display(category_projects),
            mo.md("**Payments** to those projects, by contract, from Checkbook NYC"),
            for_display(category_payments),
        ]
    )
    return (
        category_projects,
        selected_category,
        selected_projtype,
        selected_rows,
    )


@app.cell(hide_code=True)
def _():
    mo.md("""
    ### Planned and paid by fiscal year

    What each TYCS planned for the selected category in each fiscal year (blue lines, darker
    for later releases), what the selected CCP planned (gray line), and what Checkbook NYC
    paid to the category's projects (bars).

    The TYCS and CCP amounts are planned commitments. Payments come after a commitment, often
    over several years, so a year's payments mostly pay for commitments from earlier years and
    aren't expected to match that year's plans. The bars count every project listed under the
    category in any paired CCP, including what those projects were paid in other categories.
    Checkbook payments start in FY2010.
    """)
    return


@app.cell
def _(
    amounts_by_fy,
    category_picker,
    checkbook_payments,
    plan_commitments_tycs,
    plan_picker,
    selected_category,
    selected_projtype,
    selected_rows,
    tycs_rows,
):
    # what each TYCS planned for the category by fiscal year, the selected CCP's planned
    # commitments, and what was paid to the category's projects. Plans are commitments and
    # payments follow them, often for years, so the lines and bars aren't expected to match.
    # Payments cover every project listed under the category in any paired CCP, including
    # their payments in other categories.
    category_tycs_by_fy = (
        amounts_by_fy.loc[
            (amounts_by_fy["projtype"] == selected_projtype)
            & (amounts_by_fy["category"] == selected_category)
        ]
        .groupby(["fisa_version", "fy"], as_index=False)["tycs_amount"]
        .sum()
        .merge(tycs_rows[["fisa_version", "release"]].dropna(), on="fisa_version")
        .rename(columns={"tycs_amount": "amount", "release": "series"})
    )
    category_ccp_by_fy = (
        selected_rows.groupby("planned_fy", as_index=False)["amount"]
        .sum()
        .rename(columns={"planned_fy": "fy"})
        .assign(series=f"CCP {plan_picker.value} planned")
    )
    category_project_ids = plan_commitments_tycs.loc[
        (plan_commitments_tycs["projtype"] == selected_projtype)
        & (plan_commitments_tycs["category"] == selected_category),
        "maprojid",
    ].unique()
    category_paid_by_fy = (
        checkbook_payments.loc[
            checkbook_payments["maprojid"].isin(category_project_ids)
        ]
        .groupby("fiscal_year", as_index=False)["amount"]
        .sum()
        .rename(columns={"fiscal_year": "fy"})
    )
    spend_bars = (
        alt.Chart(category_paid_by_fy.assign(series="Checkbook payments"))
        .mark_bar(color="#bbbbbb")
        .encode(
            x=alt.X("fy:O", title="fiscal year"),
            y=alt.Y("amount:Q", title="$K"),
            opacity=alt.Opacity(
                "series:N", title=None, legend=alt.Legend(symbolType="square")
            ),
            tooltip=[
                "fy",
                alt.Tooltip("amount:Q", format=",.0f", title="paid ($K)"),
            ],
        )
    )
    spend_tycs = (
        alt.Chart(category_tycs_by_fy)
        .mark_line(point=True)
        .encode(
            x="fy:O",
            y="amount:Q",
            color=alt.Color(
                "series:O",
                title="TYCS release",
                sort=list(tycs_rows["release"]),
                # one hue, darker for later releases
                scale=alt.Scale(scheme={"name": "blues", "extent": [0.15, 0.75]}),
            ),
            tooltip=[
                "series",
                "fy",
                alt.Tooltip("amount:Q", format=",.0f", title="$K"),
            ],
        )
    )
    spend_ccp = (
        alt.Chart(category_ccp_by_fy)
        .mark_line(point=True)
        .encode(
            x="fy:O",
            y="amount:Q",
            color=alt.Color("series:N", title=None, scale=alt.Scale(range=["#555555"])),
            tooltip=[
                "series",
                "fy",
                alt.Tooltip("amount:Q", format=",.0f", title="$K"),
            ],
        )
    )
    (spend_bars + spend_tycs + spend_ccp).resolve_scale(color="independent").properties(
        width=650,
        title=alt.TitleParams(
            text="Planned and paid by fiscal year",
            subtitle=category_picker.value,
            fontSize=16,
            subtitleFontSize=13,
        ),
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md("""
    ### One project's history

    Pick a project from the category above. Each row is an archived CCP, oldest at the top,
    with a diamond on its release date and a circle for each of the project's planned
    commitments on its planned month (sized by amount, colored by commitment code). The bottom
    row is the project's Checkbook NYC payments by month.

    A CCP only lists commitments that are still to come, so a commitment drops out of later
    releases once it's made, and one that slips shows up at a later month. Our archived CCPs
    start in April 2021, so payments before then are paying for commitments from plans we
    don't have. A planned commitment dated before its CCP's release is one planned for earlier
    in that fiscal year that hasn't been made yet.
    """)
    return


@app.cell
def _(category_projects):
    project_picker = mo.ui.dropdown(
        options={
            f"{m}: {d}": m
            for m, d in zip(
                category_projects["maprojid"], category_projects["description"]
            )
        },
        value=next(
            f"{m}: {d}"
            for m, d in category_projects.sort_values("planned", ascending=False)[
                ["maprojid", "description"]
            ].itertuples(index=False)
        ),
        label="project",
        searchable=True,
    )
    project_picker
    return (project_picker,)


@app.cell
def _(all_ccp_commitments, ccp_rows, checkbook_payments, project_picker):
    # one project's history: its planned commitments in each archived CCP (a row per release,
    # with a dot on the release date) and its Checkbook payments by month
    project_planned = (
        all_ccp_commitments.loc[all_ccp_commitments["maprojid"] == project_picker.value]
        .groupby(["fisa_version", "planned_commit_date", "object"], as_index=False)[
            "amount"
        ]
        .sum()
        .merge(
            ccp_rows[["fisa_version", "release", "pub_date"]],
            on="fisa_version",
        )
        .assign(
            month=lambda d: pd.to_datetime(d["planned_commit_date"], format="%Y%m"),
            kind=lambda d: "planned " + d["object"],
        )
    )
    project_paid = (
        checkbook_payments.loc[checkbook_payments["maprojid"] == project_picker.value]
        .groupby("paid_month", as_index=False)["amount"]
        .sum()
        .assign(
            month=lambda d: pd.to_datetime(d["paid_month"], format="%Y-%m"),
            release="Checkbook payments",
            kind="paid",
        )
    )
    project_rows = list(project_planned.sort_values("pub_date")["release"].unique()) + [
        "Checkbook payments"
    ]
    project_points = (
        alt.Chart(
            pd.concat([project_planned, project_paid])[
                ["release", "kind", "month", "amount"]
            ]
        )
        .mark_circle(opacity=0.7)
        .encode(
            x=alt.X("month:T", title=None, axis=alt.Axis(format="%Y")),
            y=alt.Y("release:N", sort=project_rows, title=None),
            size=alt.Size(
                "amount:Q",
                title="amount ($K)",
                scale=alt.Scale(range=[40, 1200]),
            ),
            color=alt.Color("kind:N", title=None),
            tooltip=[
                "release",
                "kind",
                alt.Tooltip("month:T", format="%Y-%m"),
                alt.Tooltip("amount:Q", format=",.0f", title="amount ($K)"),
            ],
        )
    )
    project_release_dots = (
        alt.Chart(
            project_planned.drop_duplicates("release")[["release", "pub_date"]].assign(
                marker="CCP release date"
            )
        )
        .mark_point(filled=True, color="black", shape="diamond")
        .encode(
            x="pub_date:T",
            y=alt.Y("release:N", sort=project_rows),
            shape=alt.Shape("marker:N", title=None, scale=alt.Scale(range=["diamond"])),
            tooltip=["release"],
        )
    )
    (project_points + project_release_dots).properties(
        width=650,
        height=30 * len(project_rows),
        title=alt.TitleParams(
            text="Planned commitments by CCP release, and payments",
            subtitle=project_picker.selected_key,
            fontSize=16,
            subtitleFontSize=13,
        ),
    )
    return


@app.cell
def _(checkbook_payments, for_display, project_picker, whole_numbers):
    project_payments = whole_numbers(
        checkbook_payments.loc[checkbook_payments["maprojid"] == project_picker.value]
        .groupby(
            ["contract_id", "payee_name", "expense_category"],
            as_index=False,
            dropna=False,
        )
        .agg(
            first_paid=("paid_month", "min"),
            last_paid=("paid_month", "max"),
            months_paid=("paid_month", "nunique"),
            paid=("amount", "sum"),
        )
        .sort_values(["first_paid", "last_paid", "contract_id"])[
            [
                "first_paid",
                "last_paid",
                "contract_id",
                "payee_name",
                "expense_category",
                "months_paid",
                "paid",
            ]
        ],
        ["paid"],
    ).reset_index(drop=True)
    mo.vstack(
        [
            mo.md(
                f"**Payments** to {project_picker.value}, by contract, from Checkbook NYC"
            ),
            for_display(project_payments),
        ]
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## TODO: examples from the glossary

    Projects the capital planning glossary used to walk through strategy categories, CCP
    planned commitments, and Checkbook payments, from TYCS `pub_date` 20250501, CPDB
    commitments `fisa_2026`, and `nycoc_checkbook` 20260626. Rebuild them here.

    - [ ] **FDNY ePCR equipment replacement** (`057F212EPCR2`): one budget line, one category,
      one contract; payments started in March 2026, before the June 2026 planned commitment
    - [ ] **Bridges: Woodhaven Blvd over Atlantic Ave** (`841HBQ8019`): every planned
      commitment is June 2027 or later, but design payments started in June 2025
    - [ ] **HPD West 108th Street Apartments** (`806SW108`): two budget lines, one category;
      3 Checkbook payments on 2026-02-11 under `capital_project` suffixes `001`, `003`, and
      `004`, totaling the two December 2025 planned commitments
    - [ ] **QPL Mobile Library Unit** (`039LQD122VEN`): two budget lines and two contracts;
      both payments (December 2024, April 2025) came before any planned commitment
    - [ ] **Parks: Bayswater Park** (`846SANDY4-48`): one DPR project spanning 3 budget lines
      in 2 project types, including DOT's `TF-0502`, so 2 sponsor agencies; its `NFP`
      category is named differently in the TYCS and the CCP
    """)
    return


if __name__ == "__main__":
    app.run()
