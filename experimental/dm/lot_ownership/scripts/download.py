"""Download the source files into data/sources/.

PTS and PLUTO come from edm-recipes at pinned versions. HPD and NYS DOS data come from Socrata as full CSV exports,
which aren't versioned, so each file is as of the day it was downloaded. ACRIS is too big to download whole for
exploring, so only what one borough needs is downloaded, as Parquet. Reads S3 credentials from the same env vars
as dcpy (AWS_S3_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY), which the repo's root .env provides through direnv.

    python scripts/download.py          # skip files already downloaded
    python scripts/download.py --force  # download everything again
"""

import argparse
import os
import shutil
import ssl
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
import certifi
import duckdb

SOURCES_DIR = Path(__file__).parent.parent / "data" / "sources"

# local filename -> (bucket, key). Bump a version here to move to a new release.
S3_SOURCES = {
    "pluto_pts.parquet": (
        "edm-recipes",
        "datasets/pluto_pts/20261109/pluto_pts.parquet",
    ),
    "dcp_pluto.parquet": ("edm-recipes", "datasets/dcp_pluto/26v2/dcp_pluto.parquet"),
}

# local filename -> (Socrata host, dataset id)
SOCRATA_SOURCES = {
    "hpd_registrations.csv": ("data.cityofnewyork.us", "tesw-yqqr"),
    "hpd_registration_contacts.csv": ("data.cityofnewyork.us", "feu5-w2e2"),
    "nysdos_active_corporations.csv": ("data.ny.gov", "n9v6-gdp6"),
    # what each document type is, and what party 1, 2, and 3 mean for it (for a deed, grantor and grantee)
    "acris_document_control_codes.csv": ("data.cityofnewyork.us", "7isb-wh4c"),
}

ACRIS_LEGALS = "8h5j-fqxa"
ACRIS_MASTER = "bnx9-e6tj"
ACRIS_PARTIES = "636b-3b5g"
ACRIS_BOROUGH = "2"
# document types that convey ownership, from the control codes' "DEEDS AND OTHER CONVEYANCES" class. Leaves out
# contracts, leases, easements, and transfer on death deeds, whose party two doesn't own the lot yet or at all.
ACRIS_DEED_TYPES = [
    "DEED",
    "DEEDO",
    "DEED, RC",
    "DEED, LE",
    "DEEDP",
    "CORRD",
    "CONDEED",
    "IDED",
    "REIT",
]


def fetch(url: str, path: Path, quiet: bool = False) -> None:
    if not quiet:
        print(f"get     {url}")
    context = ssl.create_default_context(cafile=certifi.where())
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    # written under a temporary name, so an interrupted download never looks finished
    partial = path.with_name(path.name + ".part")
    for attempt in range(1, 4):
        try:
            # stream to disk, since the corporations export is too big to hold in memory comfortably
            with (
                urllib.request.urlopen(
                    request, context=context, timeout=300
                ) as response,
                partial.open("wb") as file,
            ):
                shutil.copyfileobj(response, file)
            break
        except OSError:
            if attempt == 3:
                raise
            print(f"retry   {path.name}")
            time.sleep(10 * attempt)
    partial.rename(path)


def fetch_soql(dataset_id: str, select: str, where: str, path: Path) -> None:
    """Download every row matching a SoQL filter to one Parquet file, a page at a time.

    Pages by document_id rather than $offset, which Socrata gets very slow at deep into a large table. A document
    can span a page boundary, so each page drops its last document_id and the next page starts from it.

    Resumes from pages an interrupted run left behind. Every row on them sorts before the id they dropped, so the
    next page starts just after the largest id they hold.
    """
    page_size = 200_000
    pages = sorted(
        path.parent.glob(f"{path.stem}.*.parquet"),
        key=lambda page: int(page.suffixes[-2].lstrip(".")),
    )
    if pages:
        after = duckdb.sql(
            f"SELECT max(document_id) FROM read_parquet({[str(p) for p in pages]})"
        ).fetchall()[0][0]
        operator = ">"
        print(f"resume  {path.name} after {len(pages)} pages")
    else:
        after = ""
        operator = ">="
    while True:
        query = urllib.parse.urlencode(
            {
                "$select": select,
                "$where": f"({where}) AND document_id {operator} '{after}'",
                "$order": "document_id",
                "$limit": page_size,
            }
        )
        page_csv = path.with_suffix(f".{len(pages)}.csv")
        print(
            f"get     {dataset_id} {path.name} page {len(pages)} from {after or 'start'}"
        )
        fetch(
            f"https://data.cityofnewyork.us/resource/{dataset_id}.csv?{query}",
            page_csv,
            quiet=True,
        )
        rows = duckdb.sql(f"SELECT * FROM read_csv('{page_csv}', all_varchar = true)")
        is_last_page = rows.count("*").fetchall()[0][0] < page_size
        if not is_last_page:
            after = rows.max("document_id").fetchall()[0][0]
            rows = rows.filter(f"document_id <> '{after}'")
        operator = ">="
        page = path.with_suffix(f".{len(pages)}.parquet")
        # written under a temporary name, so an interrupted write never looks like a finished page
        rows.write_parquet(str(page.with_name(page.name + ".part")))
        page.with_name(page.name + ".part").rename(page)
        page_csv.unlink()
        pages.append(page)
        if is_last_page:
            break
    duckdb.sql(f"SELECT * FROM read_parquet({[str(p) for p in pages]})").write_parquet(
        str(path)
    )
    for page in pages:
        page.unlink()


def write_parquet(csv_paths: list[Path], path: Path) -> None:
    files = ", ".join(f"'{p}'" for p in csv_paths)
    duckdb.sql(f"SELECT * FROM read_csv([{files}], all_varchar = true)").write_parquet(
        str(path)
    )
    for csv_path in csv_paths:
        csv_path.unlink()


def download_acris(force: bool) -> None:
    """Download what's needed to find each lot's latest deed in one borough, and who it went to.

    Deeds are downloaded citywide. Legals are only the borough's, and they decide which deeds count.

    ACRIS's parties table has no borough, so it's filtered to the documents found here, and only to grantees on
    each lot's latest deeds, to keep the download small.
    """
    # citywide, since Master's recorded_borough is 1 for most recent documents wherever the lot is
    deeds = SOURCES_DIR / "acris_deeds.parquet"
    legals = SOURCES_DIR / f"acris_{ACRIS_BOROUGH}_legals.parquet"
    grantees = SOURCES_DIR / f"acris_{ACRIS_BOROUGH}_latest_deed_grantees.parquet"
    if grantees.exists() and not force:
        print("exists  acris")
        return
    deed_types = ", ".join(f"'{t}'" for t in ACRIS_DEED_TYPES)
    if force or not deeds.exists():
        fetch_soql(
            ACRIS_MASTER,
            "document_id, doc_type, document_date, recorded_datetime, document_amt",
            f"doc_type IN ({deed_types})",
            deeds,
        )
    if force or not legals.exists():
        fetch_soql(
            ACRIS_LEGALS,
            "document_id, borough, block, lot, easement, partial_lot, property_type",
            f"borough = '{ACRIS_BOROUGH}'",
            legals,
        )
    # every deed dated on a lot's latest deed date, since one sale is often recorded as several deeds
    document_ids = [
        row[0]
        for row in duckdb.sql(
            f"""
            WITH deed_lot AS (
                SELECT DISTINCT d.document_id, l.block, l.lot,
                       coalesce(d.document_date, d.recorded_datetime)::date AS deed_date
                FROM '{deeds}' AS d JOIN '{legals}' AS l USING (document_id)
            )
            SELECT DISTINCT document_id FROM deed_lot
            QUALIFY deed_date = max(deed_date) OVER (PARTITION BY block, lot)
            ORDER BY document_id
            """
        ).fetchall()
    ]
    print(f"get     grantees of {len(document_ids)} deeds")
    chunk_size = 150  # keeps each URL a reasonable length

    def fetch_chunk(start: int) -> Path:
        ids = ", ".join(f"'{i}'" for i in document_ids[start : start + chunk_size])
        query = urllib.parse.urlencode(
            {
                "$select": "document_id, party_type, name, address_1, address_2, city, state, zip, country",
                "$where": f"party_type = '2' AND document_id IN ({ids})",
                "$limit": 50_000,
            }
        )
        chunk = grantees.with_suffix(f".{start}.csv")
        if chunk.exists():  # left by an interrupted run
            return chunk
        fetch(
            f"https://data.cityofnewyork.us/resource/{ACRIS_PARTIES}.csv?{query}",
            chunk,
            quiet=True,
        )
        return chunk

    with ThreadPoolExecutor(max_workers=4) as pool:
        chunks = list(pool.map(fetch_chunk, range(0, len(document_ids), chunk_size)))
    write_parquet(chunks, grantees)


def main(force: bool) -> None:
    client = boto3.client(
        "s3",
        endpoint_url=os.environ.get("AWS_S3_ENDPOINT"),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    SOURCES_DIR.mkdir(parents=True, exist_ok=True)
    for filename, (bucket, key) in S3_SOURCES.items():
        path = SOURCES_DIR / filename
        if path.exists() and not force:
            print(f"exists  {filename}")
            continue
        print(f"get     {bucket}/{key}")
        client.download_file(bucket, key, str(path))
    for filename, (host, dataset_id) in SOCRATA_SOURCES.items():
        path = SOURCES_DIR / filename
        if path.exists() and not force:
            print(f"exists  {filename}")
            continue
        fetch(
            f"https://{host}/api/views/{dataset_id}/rows.csv?accessType=DOWNLOAD", path
        )
    download_acris(force)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true")
    main(parser.parse_args().force)
