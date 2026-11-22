import marimo

__generated_with = "0.17.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import os
    from pathlib import Path

    import duckdb
    import marimo as mo

    LINK_SAMPLE = Path(__file__).parent.parent / "labels" / "link_sample.csv"

    # archived versions in edm-recipes, read in place
    RECIPES = {
        "dcp_pluto": "26v2",
        "dof_acris_master": "20261110",
        "dof_acris_legals": "20261110",
        "dof_acris_second_parties": "20261110",
        "hpd_registrations": "20261104",
        "hpd_registration_contacts": "20261104",
        "nysdos_active_corporations": "20261121",
    }


    def recipe(dataset: str) -> str:
        return f"s3://edm-recipes/datasets/{dataset}/{RECIPES[dataset]}/{dataset}.parquet"


    # ACRIS is only explored for this borough so far
    ACRIS_BOROUGH = "2"
    # document types that convey ownership, from the control codes' "DEEDS AND OTHER CONVEYANCES" class. Leaves out
    # contracts, leases, easements, and transfer on death deeds, whose party two doesn't own the lot yet or at all.
    ACRIS_DEED_TYPES = ["DEED", "DEEDO", "DEED, RC", "DEED, LE", "DEEDP", "CORRD", "CONDEED", "IDED", "REIT"]
    return ACRIS_BOROUGH, ACRIS_DEED_TYPES, LINK_SAMPLE, duckdb, mo, os, recipe


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # Lot ownership: feasibility

    Can we link a lot's owner name from PTS to a NYS DOS entity, and group lots by owner beyond what identical
    PTS names already show? Versions are in `RECIPES` below. ACRIS covers the Bronx only.
    """)
    return


@app.cell
def _(ACRIS_BOROUGH, ACRIS_DEED_TYPES, duckdb, os, recipe):
    con = duckdb.connect()
    # the same S3 credentials dcpy uses, from the repo's root .env
    con.execute(
        "CREATE SECRET edm_recipes (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, REGION 'us-east-1')",
        [
            os.environ["AWS_ACCESS_KEY_ID"],
            os.environ["AWS_SECRET_ACCESS_KEY"],
            os.environ["AWS_S3_ENDPOINT"].removeprefix("https://"),
        ],
    )
    # grantee addresses shared by more owner names than this are left out of linked lot counts
    # grantee and NYS process addresses used by more lot owners than this are left out of linked lot counts
    MAX_ADDRESS_OWNER_NAMES = 10
    # Names and addresses are normalized the same way everywhere: uppercase, punctuation dropped, whitespace
    # collapsed, and common suffixes spelled one way. Nothing fuzzier than that.
    con.sql(
        r"""
        CREATE OR REPLACE MACRO norm_name(s) AS nullif(trim(regexp_replace(regexp_replace(regexp_replace(
            upper(s),
            '[.,''"`]', '', 'g'),
            '[^A-Z0-9& ]', ' ', 'g'),
            '\s+', ' ', 'g')), '');
        CREATE OR REPLACE MACRO norm_entity(s) AS regexp_replace(regexp_replace(regexp_replace(regexp_replace(
            norm_name(s),
            ' LIMITED LIABILITY COMPANY$| L L C$', ' LLC'),
            ' INCORPORATED$', ' INC'),
            ' CORPORATION$', ' CORP'),
            ' LIMITED PARTNERSHIP$| L P$', ' LP');
        -- PTS splits some long names with a stray space ("CORPORATI ON"), so names are matched with spaces removed
        CREATE OR REPLACE MACRO match_key(s) AS replace(norm_entity(s), ' ', '');
        -- the words in a name that say who it is, without ones many unrelated owners share
        CREATE OR REPLACE MACRO name_words(s) AS list_filter(string_split(norm_name(s), ' '), lambda w:
            length(w) > 2 AND NOT regexp_matches(w, '^[0-9]+$') AND w NOT IN (
            'LLC', 'INC', 'CORP', 'LTD', 'LLP', 'THE', 'AND', 'REALTY', 'ASSOCIATES', 'ASSOC', 'HOLDINGS', 'PROPERTIES',
            'PROPERTY', 'MANAGEMENT', 'DEVELOPMENT', 'HOUSING', 'FUND', 'GROUP', 'PARTNERS', 'NEW', 'YORK', 'BRONX',
            'TRUST', 'TRUSTEE', 'ESTATE', 'OWNER', 'OWNERS', 'AVE', 'AVENUE', 'STREET', 'EAST', 'WEST', 'NORTH', 'SOUTH',
            'COMPANY', 'CORPORATION', 'HDFC', 'APARTMENTS'));
        -- the same without an entity suffix, for "245 PD REALTY" vs "245 PD REALTY LLC"
        CREATE OR REPLACE MACRO bare_key(s) AS replace(regexp_replace(norm_entity(s), ' (LLC|INC|CORP|CO|LP|LTD|LLP)$', ''), ' ', '');
        -- PTS and ACRIS write people "LAST, FIRST". Flipping that lines it up with "FIRST LAST". Titles and
        -- generational suffixes are dropped, so "MR. & MRS. JAMES MARTIN" and "MARTIN, JAMES JR" both become "JAMES MARTIN".
        CREATE OR REPLACE MACRO person_key(s) AS nullif(trim(regexp_replace(regexp_replace(regexp_replace(
            CASE WHEN s LIKE '%,%' THEN norm_name(split_part(s, ',', 2) || ' ' || split_part(s, ',', 1)) ELSE norm_name(s) END,
            '\b(MR|MRS|MS|DR|JR|SR|II|III|IV)\b', '', 'g'),
            '\s+', ' ', 'g'),
            '^[ &]+|[ &]+$', '', 'g')), '');
        -- house number and street only, so apartments and suites at one building compare equal
        -- "21st street 2nd floor" and "21 STREET" both become "21 ST"
        CREATE OR REPLACE MACRO norm_address(s) AS regexp_replace(regexp_replace(regexp_replace(regexp_replace(
            regexp_replace(regexp_replace(regexp_replace(norm_name(s),
            '(\s\d+(ST|ND|RD|TH))?\s+(SUITE|STE|FL|FLR|FLOOR|APT|UNIT|RM|ROOM)\b.*$', ''),
            '\b(\d+)(ST|ND|RD|TH)\b', '\1', 'g'),
            '\bAVENUE\b', 'AVE', 'g'),
            '\bSTREET\b', 'ST', 'g'),
            '\bROAD\b', 'RD', 'g'),
            '\bPLACE\b', 'PL', 'g'),
            '\bBOULEVARD\b', 'BLVD', 'g');
        """
    )
    # Materialized, since the corporations table is large and most cells read it. Raw, so regex escapes like \b
    # reach DuckDB as written.
    con.sql(
        rf"""
        CREATE OR REPLACE TABLE nys_entity AS
        SELECT
            dos_id,
            current_entity_name AS entity_name,
            norm_entity(current_entity_name) AS entity_key,
            match_key(current_entity_name) AS entity_match_key,
            bare_key(current_entity_name) AS entity_bare_key,
            entity_type,
            initial_dos_filing_date AS filing_date,
            dos_process_name AS process_name,
            concat_ws(' ', dos_process_address_1, dos_process_address_2, dos_process_city, dos_process_zip) AS process_address,
            nullif(trim(norm_address(dos_process_address_1)), '') AS process_address_key,
            ceo_name,
            registered_agent_name
        FROM '{recipe("nysdos_active_corporations")}';

        -- how many NYS entities share each process address. Registered agent services have tens of thousands.
        CREATE OR REPLACE TABLE nys_process_address AS
        SELECT process_address_key, count(*) AS nys_entities
        FROM nys_entity
        WHERE process_address_key IS NOT NULL
        GROUP BY ALL;

        CREATE OR REPLACE VIEW hpd_registration AS
        SELECT
            registrationid AS registration_id,
            concat(boroid, lpad(block, 5, '0'), lpad(lot, 4, '0')) AS bbl,
            bin,
            strptime(lastregistrationdate, '%m/%d/%Y')::date AS last_registration_date
        FROM '{recipe("hpd_registrations")}';

        CREATE OR REPLACE VIEW hpd_contact AS
        SELECT
            registrationid AS registration_id,
            type AS contact_type,
            contactdescription AS contact_description,
            corporationname AS corporation_name,
            norm_entity(corporationname) AS corporation_key,
            norm_name(concat_ws(' ', firstname, lastname)) AS person_key,
            norm_address(concat_ws(' ', businesshousenumber, businessstreetname)) AS business_address_key
        FROM '{recipe("hpd_registration_contacts")}';

        CREATE OR REPLACE TABLE acris_latest_deed_grantee AS
        WITH deed_lot AS (
            SELECT DISTINCT
                d.document_id, d.doc_type,
                concat(l.borough, lpad(l.block, 5, '0'), lpad(l.lot, 4, '0')) AS bbl,
                coalesce(d.document_date, d.recorded_datetime)::date AS deed_date
            FROM '{recipe("dof_acris_master")}' AS d
            JOIN '{recipe("dof_acris_legals")}' AS l USING (document_id)
            -- Master's recorded_borough is 1 for most recent documents wherever the lot is, so Legals sets the borough
            WHERE d.doc_type IN ({", ".join(f"'{t}'" for t in ACRIS_DEED_TYPES)}) AND l.borough = '{ACRIS_BOROUGH}'
        ),
        latest AS (
            SELECT * FROM deed_lot QUALIFY deed_date = max(deed_date) OVER (PARTITION BY bbl)
        )
        SELECT DISTINCT
            latest.bbl, latest.document_id, latest.doc_type, latest.deed_date,
            g.name, match_key(g.name) AS match_key, bare_key(g.name) AS bare_key, person_key(g.name) AS person_key,
            concat_ws(', ', g.address_1, g.address_2, g.city, g.state, g.zip) AS address,
            nullif(trim(norm_address(g.address_1)), '') AS address_key
        FROM latest JOIN '{recipe("dof_acris_second_parties")}' AS g USING (document_id);

        CREATE OR REPLACE TABLE lot AS
        WITH pluto AS (
            SELECT
                bbl::bigint::varchar AS bbl,
                left(bbl::bigint::varchar, 6) AS borough_block,
                borough, block::int AS block, lot::int AS lot, address, cd, bldgclass, landuse,
                unitsres::int AS unitsres, ownertype, ownername,
                -- placeholders PTS uses when it has no owner, mostly on condo billing lots
                CASE WHEN norm_name(ownername) NOT IN ('UNAVAILABLE OWNER', 'NAME NOT ON FILE', 'UNKNOWN')
                    THEN norm_entity(ownername) END AS owner_key,
                CASE WHEN norm_name(ownername) NOT IN ('UNAVAILABLE OWNER', 'NAME NOT ON FILE', 'UNKNOWN')
                    THEN person_key(ownername) END AS owner_person_key,
                match_key(ownername) AS owner_match_key,
                bare_key(ownername) AS owner_bare_key,
                -- names that could be an entity missing its suffix. People in PTS are mostly "LAST, FIRST".
                ownername NOT LIKE '%,%' AND regexp_matches(norm_name(ownername),
                    '[0-9]|\b(REALTY|ASSOC|ASSOCIATES|HOLDINGS?|PROPERTIES|PROPERTY|MANAGEMENT|DEVELOPMENT|HOUSING|GROUP|PARTNERS|ENTERPRISES|EQUITIES|CAPITAL|VENTURES|OWNERS?)\b')
                    AS could_be_entity_without_suffix,
                -- institutions and government bodies PLUTO doesn't mark public, which aren't people either
                ownername NOT LIKE '%,%' AND regexp_matches(norm_name(ownername),
                    '[0-9]|\b(REALTY|ASSOC|ASSOCIATES|HOLDINGS?|PROPERTIES|PROPERTY|MANAGEMENT|DEVELOPMENT|HOUSING|GROUP|PARTNERS|ENTERPRISES|EQUITIES|CAPITAL|VENTURES|OWNERS?|CORPETC|UNIVERSITY|COLLEGE|SCHOOL|CHURCH|HOSPITAL|SERVICE|SOCIETY|FOUNDATION|ASSOCIATION|AUTHORITY|CENTER|INSTITUTE|CONGREGATION|DEPARTMENT|TRUSTEES|NYS|NYC|UNITED STATES|STATE OF NEW YORK|CITY OF NEW YORK|HDFC|FUND)\b')
                    AS looks_like_an_organization,
                -- PLUTO's C, M, and O come from COLP. X only means fully tax exempt, which includes private institutions,
                -- so those lots are still classified by owner name.
                coalesce(ownertype IN ('C', 'M', 'O'), false) AS has_public_ownership,
                coalesce(ownertype = 'X', false) AS is_fully_tax_exempt,
                -- a condo billing lot's owner name is a placeholder, sponsor, or board, not the unit owners
                condono IS NOT NULL AS is_condo_billing_lot,
                regexp_matches(norm_entity(ownername), ' (LLC|INC|CORP|CO|LP|LTD|LLP)$') AS has_entity_suffix,
                regexp_matches(norm_name(ownername), '\bTRUST\b') AS has_trust_in_name
            FROM '{recipe("dcp_pluto")}'
        ),
        nys_exact AS (
            SELECT entity_match_key, any_value(dos_id) AS dos_id, any_value(entity_name) AS nys_entity_name,
                   any_value(process_address_key) AS process_address_key, count(*) AS nys_matches
            FROM nys_entity
            WHERE entity_match_key IN (SELECT owner_match_key FROM pluto)
            GROUP BY entity_match_key
        ),
        nys_bare AS (
            SELECT entity_bare_key, any_value(dos_id) AS dos_id, any_value(entity_name) AS nys_entity_name,
                   any_value(process_address_key) AS process_address_key, count(*) AS nys_matches
            FROM nys_entity
            WHERE entity_bare_key IN (SELECT owner_bare_key FROM pluto WHERE could_be_entity_without_suffix)
            GROUP BY entity_bare_key
        ),
        -- an exact match first, then one without the suffix when the name could be an entity missing it
        nys_match AS (
            SELECT pluto.bbl,
                   coalesce(nys_exact.nys_matches, nys_bare.nys_matches) AS nys_matches,
                   CASE WHEN nys_exact.nys_matches IS NOT NULL THEN 'exact' WHEN nys_bare.nys_matches IS NOT NULL THEN 'without suffix' END AS nys_match_method,
                   coalesce(nys_exact.dos_id, nys_bare.dos_id) AS dos_id,
                   coalesce(nys_exact.nys_entity_name, nys_bare.nys_entity_name) AS nys_entity_name,
                   coalesce(nys_exact.process_address_key, nys_bare.process_address_key) AS process_address_key
            FROM pluto
            LEFT JOIN nys_exact ON pluto.owner_match_key = nys_exact.entity_match_key
            LEFT JOIN nys_bare ON pluto.could_be_entity_without_suffix AND pluto.owner_bare_key = nys_bare.entity_bare_key
            WHERE nys_exact.nys_matches IS NOT NULL OR nys_bare.nys_matches IS NOT NULL
        ),
        latest_deed AS (
            SELECT bbl, max(deed_date) AS latest_deed_date, count(DISTINCT name) AS latest_deed_grantees,
                   list(DISTINCT match_key) AS grantee_match_keys, list(DISTINCT bare_key) AS grantee_bare_keys,
                   list(DISTINCT person_key) AS grantee_person_keys
            FROM acris_latest_deed_grantee
            GROUP BY bbl
        )
        SELECT
            pluto.* EXCLUDE (has_entity_suffix, has_trust_in_name, could_be_entity_without_suffix, looks_like_an_organization),
            NOT has_public_ownership AND owner_key IS NULL AS has_no_owner_name,
            NOT has_public_ownership AND owner_key IS NOT NULL
                AND (has_entity_suffix OR looks_like_an_organization OR nys_matches IS NOT NULL) AS is_owned_by_an_entity,
            NOT has_public_ownership AND owner_key IS NOT NULL
                AND has_trust_in_name AND NOT (has_entity_suffix OR looks_like_an_organization OR nys_matches IS NOT NULL) AS is_owned_by_a_trust,
            NOT has_public_ownership AND owner_key IS NOT NULL
                AND NOT has_trust_in_name AND NOT (has_entity_suffix OR looks_like_an_organization OR nys_matches IS NOT NULL) AS is_owned_by_an_individual,
            coalesce(nys_matches = 1, false) AS has_one_nys_entity,
            coalesce(nys_matches > 1, false) AS has_several_nys_entities,
            bbl IN (SELECT bbl FROM hpd_registration) AS has_hpd_registration,
            -- only kept when the name matches exactly one DOS ID
            CASE WHEN nys_matches = 1 THEN nys_match_method END AS nys_match_method,
            CASE WHEN nys_matches = 1 THEN nys_match.nys_entity_name END AS nys_entity_name,
            CASE WHEN nys_matches = 1 THEN nys_match.dos_id END AS dos_id,
            CASE WHEN nys_matches = 1 THEN nys_process_address.nys_entities END AS process_address_nys_entities,
            borough = 'BX' AS has_acris_coverage,
            latest_deed.latest_deed_date,
            latest_deed.latest_deed_grantees,
            CASE WHEN latest_deed.bbl IS NOT NULL
                THEN list_contains(grantee_match_keys, owner_match_key) OR list_contains(grantee_bare_keys, owner_bare_key)
                    OR list_contains(grantee_person_keys, owner_person_key)
            END AS is_pts_owner_a_latest_grantee
        FROM pluto
        LEFT JOIN nys_match USING (bbl)
        LEFT JOIN nys_process_address USING (process_address_key)
        LEFT JOIN latest_deed USING (bbl);

        -- grantee addresses on latest deeds: how many owner names use each, and whether it's a government address
        CREATE OR REPLACE TABLE grantee_address AS
        SELECT
            g.address_key,
            count(DISTINCT g.bbl) AS lots,
            count(DISTINCT coalesce(CASE WHEN lot.is_owned_by_an_individual THEN lot.owner_person_key END, lot.owner_key)) AS owner_names,
            -- a government grantee, not a private entity with "NYC" in its name
            bool_or(regexp_matches(norm_name(g.name),
                '\b(CITY OF NEW YORK|NEW YORK CITY|NYC|STATE OF NEW YORK|PEOPLE OF THE STATE|UNITED STATES|HOUSING AUTHORITY|TRANSPORTATION AUTHORITY|DEPARTMENT|DEPT)\b')
                AND NOT regexp_matches(norm_entity(g.name), ' (LLC|INC|CORP|CO|LP|LTD|LLP)$'))
                AS is_government_address
        FROM acris_latest_deed_grantee AS g JOIN lot USING (bbl)
        WHERE g.address_key IS NOT NULL
        GROUP BY g.address_key;

        -- lots owned by an individual whose grantee address link counts: another owner at the address shares a
        -- name word. Otherwise it's usually a buyer who lived at that address, often in an apartment building.
        CREATE OR REPLACE TABLE grantee_address_named_individual AS
        WITH address_lot AS (
            SELECT DISTINCT g.address_key, lot.bbl, lot.ownername, lot.is_owned_by_an_individual
            FROM acris_latest_deed_grantee AS g JOIN lot USING (bbl)
            WHERE g.address_key IS NOT NULL
        )
        SELECT DISTINCT a.address_key, a.bbl
        FROM address_lot AS a JOIN address_lot AS b ON a.address_key = b.address_key AND a.bbl <> b.bbl
        WHERE a.is_owned_by_an_individual AND list_has_any(name_words(a.ownername), name_words(b.ownername));

        -- lots whose owning entity uses a NYS process address, and entity-owned lots at that address, like an
        -- owner's office. Only lot owners are counted at each address: a law office there that owns no lots
        -- can't link anything.
        CREATE OR REPLACE TABLE process_address_lot AS
        SELECT DISTINCT bbl, process_address_key
        FROM (
            SELECT lot.bbl, e.process_address_key FROM lot JOIN nys_entity AS e USING (dos_id)
            WHERE e.process_address_key IS NOT NULL
            UNION ALL
            SELECT lot.bbl, norm_address(lot.address) FROM lot
            WHERE (lot.is_owned_by_an_entity OR lot.is_owned_by_a_trust)
              AND norm_address(lot.address) IN (SELECT e.process_address_key FROM lot JOIN nys_entity AS e USING (dos_id))
        );
        CREATE OR REPLACE TABLE process_address AS
        SELECT p.process_address_key, count(DISTINCT lot.owner_match_key) AS owner_names, any_value(a.nys_entities) AS nys_entities
        FROM process_address_lot AS p JOIN lot USING (bbl) JOIN nys_process_address AS a USING (process_address_key)
        GROUP BY p.process_address_key;

        -- one row per lot and thing it shares with other lots. is_counted leaves out addresses that are a
        -- government's or that many lot owners share, which link unrelated owners.
        CREATE OR REPLACE TABLE lot_link AS
        SELECT bbl, borough_block, 'dos_id' AS link_type, dos_id AS link_key,
               NULL::INTEGER AS address_owner_names, NULL::INTEGER AS address_nys_entities, true AS is_counted
        FROM lot WHERE dos_id IS NOT NULL
        UNION ALL
        SELECT bbl, borough_block, 'pts_person_name', owner_person_key, NULL, NULL, true
        FROM lot WHERE is_owned_by_an_individual
        UNION ALL
        SELECT bbl, borough_block, 'pts_entity_name', owner_match_key, NULL, NULL, true
        FROM lot WHERE (is_owned_by_an_entity OR is_owned_by_a_trust) AND dos_id IS NULL
        UNION ALL
        SELECT DISTINCT lot.bbl, lot.borough_block, 'latest_deed_grantee_address', g.address_key, a.owner_names, NULL,
               NOT a.is_government_address AND a.owner_names <= {MAX_ADDRESS_OWNER_NAMES}
                   AND (NOT lot.is_owned_by_an_individual OR n.bbl IS NOT NULL)
        FROM acris_latest_deed_grantee AS g JOIN lot USING (bbl) JOIN grantee_address AS a USING (address_key)
        LEFT JOIN grantee_address_named_individual AS n ON n.address_key = g.address_key AND n.bbl = lot.bbl
        UNION ALL
        SELECT lot.bbl, lot.borough_block, 'nys_process_address', p.process_address_key, a.owner_names, a.nys_entities,
               a.owner_names <= {MAX_ADDRESS_OWNER_NAMES}
        FROM process_address_lot AS p JOIN lot USING (bbl) JOIN process_address AS a USING (process_address_key);

        -- how many other lots each lot shares a counted link with, anywhere and on its own tax block, and
        -- whether through an owner name (a DOS ID or the same normalized name) or an address
        ALTER TABLE lot ADD COLUMN linked_lots INTEGER DEFAULT 0;
        ALTER TABLE lot ADD COLUMN linked_lots_on_block INTEGER DEFAULT 0;
        ALTER TABLE lot ADD COLUMN is_linked_by_owner_name BOOLEAN DEFAULT false;
        ALTER TABLE lot ADD COLUMN is_linked_by_owner_name_on_block BOOLEAN DEFAULT false;
        ALTER TABLE lot ADD COLUMN is_linked_by_address BOOLEAN DEFAULT false;
        ALTER TABLE lot ADD COLUMN is_linked_by_address_on_block BOOLEAN DEFAULT false;
        UPDATE lot SET
            linked_lots = c.linked_lots,
            linked_lots_on_block = c.linked_lots_on_block,
            is_linked_by_owner_name = c.is_linked_by_owner_name,
            is_linked_by_owner_name_on_block = c.is_linked_by_owner_name_on_block,
            is_linked_by_address = c.is_linked_by_address,
            is_linked_by_address_on_block = c.is_linked_by_address_on_block
        FROM (
            SELECT a.bbl,
                   count(DISTINCT b.bbl) AS linked_lots,
                   count(DISTINCT b.bbl) FILTER (WHERE a.borough_block = b.borough_block) AS linked_lots_on_block,
                   bool_or(NOT a.link_type LIKE '%address') AS is_linked_by_owner_name,
                   bool_or(NOT a.link_type LIKE '%address' AND a.borough_block = b.borough_block) AS is_linked_by_owner_name_on_block,
                   bool_or(a.link_type LIKE '%address') AS is_linked_by_address,
                   bool_or(a.link_type LIKE '%address' AND a.borough_block = b.borough_block) AS is_linked_by_address_on_block
            FROM lot_link AS a
            JOIN lot_link AS b USING (link_type, link_key)
            WHERE a.bbl <> b.bbl AND a.is_counted AND b.is_counted
            GROUP BY a.bbl
        ) AS c
        WHERE lot.bbl = c.bbl;
        """
    )

    def q(sql: str):
        return con.sql(sql).df()

    return (q,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Owner kinds

    Each lot gets one of these flags, so a null `dos_id` reads as "owned by a person", not "match failed".
    An entity is a name with an entity suffix or one that exactly matches a NYS entity name.
    """)
    return


@app.cell
def _(q):
    q(
        """
        SELECT
            has_public_ownership, is_owned_by_an_entity, is_owned_by_a_trust, is_owned_by_an_individual, has_no_owner_name,
            count(*) AS lots,
            round(100 * count(*) / sum(count(*)) OVER (), 1) AS pct
        FROM lot
        GROUP BY ALL
        ORDER BY lots DESC
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## PTS owner name to NYS entity

    Exact match on the normalized name. A name matching more than one DOS ID is left unmatched.
    """)
    return


@app.cell
def _(q):
    q(
        """
        SELECT
            count(*) AS entity_lots,
            count(*) FILTER (WHERE has_one_nys_entity) AS one_dos_id,
            count(*) FILTER (WHERE has_several_nys_entities) AS several_dos_ids,
            round(100 * count(*) FILTER (WHERE has_one_nys_entity) / count(*), 1) AS pct_one_dos_id
        FROM lot
        WHERE is_owned_by_an_entity
        """
    )
    return


@app.cell
def _(q):
    q(
        """
        -- most common unmatched entity owners, to see why they miss (split words, out-of-state, dissolved)
        SELECT owner_key, count(*) AS lots
        FROM lot
        WHERE is_owned_by_an_entity AND NOT has_one_nys_entity
        GROUP BY ALL
        ORDER BY lots DESC
        LIMIT 50
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Shared process addresses

    How many NYS entities share the process address of a lot-owning entity. Registered agent services and law
    firms make this a weak link on its own, so it's kept as a count to filter on, not used to group.
    """)
    return


@app.cell
def _(q):
    q(
        """
        SELECT
            CASE
                WHEN process_address_nys_entities = 1 THEN '1'
                WHEN process_address_nys_entities <= 3 THEN '2-3'
                WHEN process_address_nys_entities <= 10 THEN '4-10'
                WHEN process_address_nys_entities <= 100 THEN '11-100'
                ELSE 'over 100'
            END AS nys_entities_at_process_address,
            count(DISTINCT dos_id) AS lot_owning_entities,
            count(*) AS lots
        FROM lot
        WHERE dos_id IS NOT NULL AND process_address_nys_entities IS NOT NULL
        GROUP BY ALL
        ORDER BY min(process_address_nys_entities)
        """
    )
    return


@app.cell
def _(q):
    q(
        """
        SELECT n.process_address_key, a.nys_entities, count(DISTINCT lot.dos_id) AS lot_owning_entities,
               count(*) AS lots, count(DISTINCT lot.borough_block) AS blocks
        FROM lot
        JOIN nys_entity AS n USING (dos_id)
        JOIN nys_process_address AS a USING (process_address_key)
        GROUP BY ALL
        ORDER BY a.nys_entities DESC
        LIMIT 20
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## HPD registration coverage

    HPD registers multiple dwellings, so this covers residential buildings with three or more units, not
    everything.
    """)
    return


@app.cell
def _(q):
    q(
        """
        SELECT
            unitsres >= 3 AS has_three_plus_units,
            count(*) AS lots,
            count(*) FILTER (WHERE has_hpd_registration) AS lots_registered,
            round(100 * count(*) FILTER (WHERE has_hpd_registration) / count(*), 1) AS pct_registered
        FROM lot
        GROUP BY ALL
        ORDER BY ALL
        """
    )
    return


@app.cell
def _(q):
    q(
        """
        -- the head officers spanning the most PTS owner names, to eyeball whether they're owners or managers
        WITH head_officer AS (
            SELECT DISTINCT r.bbl, c.person_key
            FROM hpd_registration AS r JOIN hpd_contact AS c USING (registration_id)
            WHERE c.contact_type = 'HeadOfficer' AND c.person_key IS NOT NULL
        )
        SELECT person_key, count(DISTINCT bbl) AS lots, count(DISTINCT owner_key) AS pts_owner_names,
               count(DISTINCT borough_block) AS blocks, list(DISTINCT owner_key)[:5] AS example_owner_names
        FROM head_officer JOIN lot USING (bbl)
        GROUP BY ALL
        ORDER BY pts_owner_names DESC
        LIMIT 30
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## ACRIS: PTS owner vs latest deed (Bronx)

    `is_pts_owner_a_latest_grantee` is false where PTS names someone the latest deed didn't go to, and null where
    there's no deed to compare (condo units, lots with no deed of the types in `ACRIS_DEED_TYPES`).
    """)
    return


@app.cell
def _(q):
    q(
        """
        SELECT
            is_owned_by_an_entity, is_owned_by_a_trust, is_owned_by_an_individual,
            is_pts_owner_a_latest_grantee,
            count(*) AS lots,
            round(100 * count(*) / sum(count(*)) OVER (PARTITION BY is_owned_by_an_entity, is_owned_by_a_trust, is_owned_by_an_individual), 1) AS pct_of_kind,
            round(avg(latest_deed_grantees), 2) AS avg_grantees
        FROM lot
        WHERE has_acris_coverage AND NOT has_public_ownership AND NOT has_no_owner_name
        GROUP BY 1, 2, 3, 4
        ORDER BY ALL
        """
    )
    return


@app.cell
def _(q):
    q(
        """
        -- the example block
        SELECT lot.bbl, lot.ownername AS pts_owner, lot.latest_deed_date, lot.is_pts_owner_a_latest_grantee,
               list(DISTINCT g.name ORDER BY g.name) AS grantees,
               list(DISTINCT g.address ORDER BY g.address) AS grantee_addresses
        FROM lot LEFT JOIN acris_latest_deed_grantee AS g USING (bbl)
        WHERE lot.borough_block = '205109'
        GROUP BY ALL
        ORDER BY lot.bbl
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Links between lots

    Each row says two or more lots share something: a DOS ID, a person's name, or a grantee address on their
    latest deeds. Whether the lots are on one block is a column, not a filter, since common ownership can cross
    blocks and two neighbors can share a name by chance.
    """)
    return


@app.cell
def _(q):
    q(
        """
        WITH link_group AS (
            SELECT link_type, link_key, count(DISTINCT bbl) AS lots, count(DISTINCT borough_block) AS blocks
            FROM lot_link
            GROUP BY ALL
            HAVING count(DISTINCT bbl) > 1
        )
        SELECT
            link_type,
            blocks = 1 AS is_one_block,
            count(*) AS groups,
            sum(lots) AS lots,
            max(lots) AS largest_group
        FROM link_group
        GROUP BY ALL
        ORDER BY ALL
        """
    )
    return


@app.cell
def _(q):
    q(
        """
        -- grantee addresses shared by the most lots: owners' offices, or a lender's or lawyer's address
        SELECT link_key AS grantee_address, count(DISTINCT bbl) AS lots, count(DISTINCT borough_block) AS blocks,
               list(DISTINCT lot.ownername)[:5] AS example_pts_owners
        FROM lot_link JOIN lot USING (bbl, borough_block)
        WHERE link_type = 'latest_deed_grantee_address'
        GROUP BY ALL
        ORDER BY lots DESC
        LIMIT 30
        """
    )
    return


@app.cell
def _(q):
    q(
        """
        -- the Zottola lots, found through the grantee address on lots 1 and 4
        SELECT lot.bbl, lot.borough_block, lot.address, lot.ownername, lot.dos_id, l.link_type
        FROM lot_link AS l JOIN lot USING (bbl, borough_block)
        WHERE (l.link_type = 'latest_deed_grantee_address' AND l.link_key = '2005 HOBART AVE')
           OR l.link_key IN (SELECT dos_id FROM nys_entity WHERE process_address_key = '2005 HOBART AVE')
        ORDER BY lot.bbl
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Summary (Bronx)

    Every Bronx lot in exactly one group, checked top to bottom, so the groups add up to all Bronx lots.
    A link through an owner name means a DOS ID or the same normalized owner name, which PLUTO already shows.
    A link through an address means a grantee or NYS process address, which PLUTO doesn't.
    """)
    return


@app.cell
def _(q):
    q(
        """
        WITH grouped AS (
            SELECT CASE
                WHEN has_public_ownership THEN 'A. public ownership'
                WHEN has_no_owner_name THEN 'B. no owner name'
                WHEN is_linked_by_owner_name_on_block THEN 'C. linked on its tax block through an owner name'
                WHEN is_linked_by_address_on_block THEN 'D. linked on its tax block only through an address'
                WHEN is_linked_by_owner_name THEN 'E. linked only off its tax block, through an owner name'
                WHEN is_linked_by_address THEN 'F. linked only off its tax block, only through an address'
                ELSE 'G. not linked to another lot'
            END AS lot_group
            FROM lot
            WHERE borough = 'BX'
        )
        SELECT lot_group, count(*) AS lots, round(100 * count(*) / sum(count(*)) OVER (), 1) AS pct
        FROM grouped
        GROUP BY ALL
        UNION ALL
        SELECT 'total', count(*), 100.0 FROM grouped
        ORDER BY lot_group
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    The same lots by kind of link. These overlap: a lot can have several kinds of link.
    """)
    return


@app.cell
def _(q):
    q(
        """
        WITH bx AS (SELECT bbl, borough_block FROM lot WHERE borough = 'BX'),
        lot_link_type AS (
            SELECT a.bbl, a.link_type, bool_or(a.borough_block = b.borough_block) AS is_on_block
            FROM lot_link AS a JOIN lot_link AS b USING (link_type, link_key)
            WHERE a.is_counted AND b.is_counted AND a.bbl <> b.bbl AND a.bbl IN (SELECT bbl FROM bx)
            GROUP BY ALL
        )
        SELECT
            link_type,
            count(*) AS lots_linked,
            round(100 * count(*) / (SELECT count(*) FROM bx), 1) AS pct_of_bronx_lots,
            count(*) FILTER (WHERE is_on_block) AS lots_linked_on_block,
            round(100 * count(*) FILTER (WHERE is_on_block) / (SELECT count(*) FROM bx), 1) AS pct_on_block
        FROM lot_link_type
        GROUP BY ALL
        ORDER BY lots_linked DESC
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Labeling links

    A fixed sample of 100 pairs of lots that share a link, to measure how often a link means the same owner.
    It's written to `labels/link_sample.csv` once and never overwritten. Fill in `is_same_owner` with `yes`, `no`,
    or `unsure`, and the next cell scores it.

    | Kind of link | Pairs | Why |
    |---|---|---|
    | grantee address, different owner names | 40 | the link most likely to add owners PLUTO doesn't show |
    | NYS process address, different DOS IDs, 10 or fewer entities there | 20 | not used yet. It's what connects the PLAX LLCs |
    | person name, different blocks | 20 | the "JOHN SMITH" problem |
    | person name, same block | 20 | |
    """)
    return


@app.cell
def _(LINK_SAMPLE, q):
    _sample = q(
        """
        WITH address_pair AS (
            SELECT 'grantee address' AS kind, a.bbl AS bbl_a, b.bbl AS bbl_b, a.link_key AS shared
            FROM lot_link AS a JOIN lot_link AS b USING (link_type, link_key)
            JOIN lot AS la ON la.bbl = a.bbl JOIN lot AS lb ON lb.bbl = b.bbl
            WHERE a.link_type = 'latest_deed_grantee_address' AND a.is_counted AND b.is_counted AND a.bbl < b.bbl
              AND coalesce(la.owner_person_key, '') <> coalesce(lb.owner_person_key, '')
              AND la.owner_match_key <> lb.owner_match_key
            ORDER BY hash(a.bbl || b.bbl) LIMIT 40
        ),
        process_pair AS (
            SELECT 'process address' AS kind, la.bbl AS bbl_a, lb.bbl AS bbl_b, ea.process_address_key AS shared
            FROM lot AS la JOIN nys_entity AS ea ON ea.dos_id = la.dos_id
            JOIN nys_entity AS eb ON eb.process_address_key = ea.process_address_key AND eb.dos_id > ea.dos_id
            JOIN lot AS lb ON lb.dos_id = eb.dos_id
            WHERE la.process_address_nys_entities <= 10
            QUALIFY row_number() OVER (PARTITION BY ea.process_address_key ORDER BY hash(la.bbl || lb.bbl)) = 1
            ORDER BY hash(la.bbl || lb.bbl) LIMIT 20
        ),
        person_pair AS (
            SELECT a.borough_block = b.borough_block AS is_same_block, a.bbl AS bbl_a, b.bbl AS bbl_b, a.link_key AS shared
            FROM lot_link AS a JOIN lot_link AS b USING (link_type, link_key)
            WHERE a.link_type = 'pts_person_name' AND a.bbl < b.bbl
            -- one pair per name, so a few common names don't fill the sample
            QUALIFY row_number() OVER (PARTITION BY a.link_key, a.borough_block = b.borough_block ORDER BY hash(a.bbl || b.bbl)) = 1
        ),
        pair AS (
            SELECT * FROM address_pair
            UNION ALL SELECT * FROM process_pair
            UNION ALL (SELECT 'person name, different blocks', bbl_a, bbl_b, shared FROM person_pair
                       WHERE NOT is_same_block ORDER BY hash(bbl_a || bbl_b) LIMIT 20)
            UNION ALL (SELECT 'person name, same block', bbl_a, bbl_b, shared FROM person_pair
                       WHERE is_same_block ORDER BY hash(bbl_a || bbl_b) LIMIT 20)
        )
        SELECT
            pair.kind, pair.shared, pair.bbl_a, pair.bbl_b,
            la.ownername AS owner_a, lb.ownername AS owner_b,
            la.address AS address_a, lb.address AS address_b,
            la.borough_block = lb.borough_block AS is_same_block,
            NULL AS is_same_owner, NULL AS note
        FROM pair JOIN lot AS la ON la.bbl = pair.bbl_a JOIN lot AS lb ON lb.bbl = pair.bbl_b
        ORDER BY pair.kind, pair.bbl_a
        """
    )
    if not LINK_SAMPLE.exists():
        LINK_SAMPLE.parent.mkdir(exist_ok=True)
        _sample.to_csv(LINK_SAMPLE, index=False)
    # the saved sample, which differs from a fresh one once the links change
    q(f"SELECT * FROM read_csv('{LINK_SAMPLE}', all_varchar = true)")
    return


@app.cell
def _(LINK_SAMPLE, mo, q):
    mo.stop(not LINK_SAMPLE.exists())
    q(
        f"""
        SELECT
            kind,
            count(*) AS pairs,
            count(*) FILTER (WHERE is_same_owner = 'yes') AS yes,
            count(*) FILTER (WHERE is_same_owner = 'no') AS no,
            count(*) FILTER (WHERE is_same_owner = 'unsure') AS unsure,
            count(*) FILTER (WHERE is_same_owner IS NULL) AS unlabeled,
            round(100 * count(*) FILTER (WHERE is_same_owner = 'yes')
                / nullif(count(*) FILTER (WHERE is_same_owner IN ('yes', 'no')), 0), 1) AS pct_same_owner
        FROM read_csv('{LINK_SAMPLE}', all_varchar = true)
        GROUP BY kind
        ORDER BY kind
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Draft output

    One row per lot. Nulls in `dos_id`, `person_key`, and the deed columns are explained by the flags.
    """)
    return


@app.cell
def _(q):
    q(
        """
        SELECT
            bbl, borough_block, ownername AS owner_name,
            has_public_ownership, is_owned_by_an_entity, is_owned_by_a_trust, is_owned_by_an_individual, has_no_owner_name,
            is_fully_tax_exempt, is_condo_billing_lot,
            CASE WHEN is_owned_by_an_individual THEN owner_person_key END AS person_key,
            nys_entity_name, dos_id, process_address_nys_entities,
            has_hpd_registration, has_acris_coverage, latest_deed_date, latest_deed_grantees, is_pts_owner_a_latest_grantee,
            linked_lots, linked_lots_on_block
        FROM lot
        WHERE borough_block IN ('204231', '205109')
        ORDER BY bbl
        """
    )
    return


if __name__ == "__main__":
    app.run()
