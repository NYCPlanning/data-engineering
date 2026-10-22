"""
The LDF's segment records: 'S' (base centerline), 'P' (physical) and 'G' (generic).

Unlike node records these are not derived by diffing LION. CSCL journals every centerline
edit in CENTERLINEHISTORY and the LDF publishes that journal, so two things happen here:

1. Select the edition. Rows for the release being cut carry no release_num - GR stamps it
   at publish time, after the GDB we ingest was snapshotted.
2. Eliminate transitory records: edits that were undone or superseded before the release,
   which LION users never saw.

Step 2 is a port of prod's rule, CSCL_Editor.LDFExtractHelper in CSCLEditor.dll (shipped
in the prod ETL archive). It is sequential: rows are visited in journal order and flagged
as they go, and later decisions read earlier flags, so it is written as Python rather
than SQL. Method names below follow prod's so the two can be read side by side.

One deliberate deviation (CSCL-LDF-01): prod drops any G or P record whose generic or
physical ID is deleted anywhere in the journal, even on a different segment. A generic or
physical ID spans many segments, so that drops reassignments LION shows took effect. Here
a G or P record is dropped only when its ID is deleted on the same node pair.
"""

import pandas as pd

COLUMNS = [
    "record_type",
    "action_code",
    "old_id",
    "old_from_nodeid",
    "old_to_nodeid",
    "new_id",
    "new_from_nodeid",
    "new_to_nodeid",
]


def _text(value):
    """A value as prod's DataTable renders it: null is the empty string."""
    return "" if value is None else str(value)


class TransitoryFilter:
    """
    Prod's transitory-record elimination. Each journal row becomes a dict:

    - ldf_num, action, nodes (ofn/otn/nfn/ntn) as journalled
    - old: old ID, None when blank
    - new: new ID, 0 when blank (prod's convention, so a blank never matches an ID)
    - new_old: the ID published in the old-ID position; starts as `old` and is
      rewritten when a chain of splits and merges is collapsed
    - status: '' (undecided), 'Y' (publish) or 'N' (transitory)
    """

    def __init__(self, journal):
        self.journal = journal
        # IDs deleted anywhere in the journal. Prod keeps one set for the whole run, so
        # it accumulates S, then G, then P deletes.
        self.deleted_ids = set()
        self.deleted_on_pair = set()
        self.rows = []
        self.removed = set()
        self.record_type = None

    # -- CreateTransitoryDataTable ------------------------------------------------------
    def load(self, record_type):
        self.record_type = record_type
        self.rows = []
        for r in self.journal:
            if r["record_type"] != record_type:
                continue
            row = {
                "ldf_num": r["ldf_num"],
                "action": (r["action_code"] or "").strip(),
                "old": r["old_id"],
                "new": r["new_id"] if r["new_id"] is not None else 0,
                "new_old": r["old_id"],
                "status": "",
                "ofn": r["old_from_nodeid"],
                "otn": r["old_to_nodeid"],
                "nfn": r["new_from_nodeid"],
                "ntn": r["new_to_nodeid"],
            }
            self.rows.append(row)
            old = _text(row["old"])
            if row["action"].upper() == "D" and old not in ("", "0"):
                self.deleted_ids.add(old)
                self.deleted_on_pair.add(
                    (record_type, old, frozenset((row["ofn"], row["otn"])))
                )
                if _text(row["new"]) != "0":
                    self.deleted_ids.add(_text(row["new"]))

    def _select(self, predicate, by_ldf_num=False):
        found = [r for r in self.rows if predicate(r)]
        return sorted(found, key=lambda r: r["ldf_num"]) if by_ldf_num else found

    @staticmethod
    def _require(*values):
        # Prod builds DataTable filters by string concatenation; a blank value makes the
        # filter unparseable and aborts the whole extract. Fail the same way, loudly.
        if "" in values:
            raise ValueError(
                "LDF transitory processing hit a blank ID or node in a filter"
            )

    # -- S records: ProcessTrasitoryrecords --------------------------------------------
    def _has_split(self, seg_id):
        rows = self._select(lambda r: _text(r["old"]) == seg_id and r["action"] == "S")
        if (
            len(rows) == 2
            and _text(rows[0]["new"]) != "0"
            and _text(rows[1]["new"]) != "0"
        ):
            return True, rows, _text(rows[0]["new"]), _text(rows[1]["new"])
        return False, rows, "", ""

    def _has_merge(self, id1, id2):
        rows = self._select(
            lambda r: _text(r["old"]) in (id1, id2) and r["action"] == "M"
        )
        if len(rows) == 2:
            a, b = _text(rows[0]["new"]), _text(rows[1]["new"])
            if a != "0" and b != "0" and a == b:
                return True, rows, a
        return False, rows, ""

    def _collapse_chain(self, chain, start_id, is_split):
        """A split/merge chain that returns to a single segment publishes only its last step."""
        if len(chain) <= 2:
            return
        if len(chain) % 2 == 0:
            kept, first_dropped = chain[-2], len(chain) - 3
        else:
            kept, first_dropped = chain[-1], len(chain) - 2
        for r in kept[:2]:
            r["new_old"] = int(start_id)
            r["status"] = "Y"
        for step in chain[first_dropped::-1]:
            step[0]["status"] = "N"
            self.removed.add(step[0]["ldf_num"])
            if is_split:
                # Prod flags the first row twice and never the second, but removes both.
                self.removed.add(step[1]["ldf_num"])
            elif len(step) > 1:
                step[1]["status"] = "N"
                self.removed.add(step[1]["ldf_num"])

    def process_segments(self):
        self.removed = set()
        for row in self.rows:
            old = _text(row["old"])
            created = (
                self._select(lambda r, old=old: _text(r["new"]) == old) if old else []
            )
            if row["action"] in ("S", "M") and not created:
                current = old
            else:
                current = _text(row["new"])
            if row["ldf_num"] in self.removed or row["status"] != "":
                continue
            self._require(current)
            successors = self._select(
                lambda r, c=current: _text(r["old"]) == c, by_ldf_num=True
            )
            for succ in successors:
                if _text(succ["new"]) == "0":
                    continue
                if succ["action"] == "D":
                    row["status"] = "N"
                    self.removed.add(succ["ldf_num"])
                    break
                if succ["action"] == "S":
                    if len(successors) < 2:
                        raise ValueError(
                            "LDF transitory processing: split with one child"
                        )
                    chain = [successors]
                    a, b = _text(successors[0]["new"]), _text(successors[1]["new"])
                    while True:
                        merged, rows, into = self._has_merge(a, b)
                        if not merged:
                            break
                        chain.append(rows)
                        split, rows, a, b = self._has_split(into)
                        if not split:
                            break
                        chain.append(rows)
                    self._collapse_chain(chain, current, is_split=True)
                elif succ["action"] == "M":
                    chain = [successors]
                    into = _text(successors[0]["new"])
                    while True:
                        split, rows, a, b = self._has_split(into)
                        if not split:
                            break
                        chain.append(rows)
                        merged, rows, into = self._has_merge(a, b)
                        if not merged:
                            break
                        chain.append(rows)
                    self._collapse_chain(chain, current, is_split=False)

        # A deleted segment takes the records that created it with it - and, for a split
        # whose other child was also deleted, the records that created the parent.
        transitory = []
        for d in self._select(lambda r: r["action"] == "D" and r["status"] != "N"):
            gone = _text(d["old"]).strip()
            if gone in ("", "0"):
                continue
            creators = self._select(
                lambda r, g=gone: _text(r["new"]) == g and r["status"] != "N"
            )
            if not creators:
                continue
            transitory.append(d)
            for creator in creators:
                transitory.append(creator)
                if creator["action"] != "S":
                    continue
                self._require(_text(creator["old"]))
                siblings = self._select(
                    lambda r, c=creator: (
                        _text(r["old"]) == _text(c["old"])
                        and _text(r["new"]) != _text(c["new"])
                        and r["action"] == "S"
                    )
                )
                if not siblings or _text(siblings[0]["new"]) in ("", "0"):
                    continue
                sibling_deletes = self._select(
                    lambda r, s=siblings[0]: (
                        _text(r["old"]) == _text(s["new"]) and r["action"] == "D"
                    )
                )
                if not sibling_deletes or _text(sibling_deletes[0]["old"]) in ("", "0"):
                    continue
                transitory += self._select(
                    lambda r, s=siblings[0]: _text(r["new"]) == _text(s["old"])
                )
        for r in transitory:
            r["status"] = "N"

    # -- G and P records: ProcessGenPhyTransitoryRecords --------------------------------
    def _find_related(self, row, related):
        """Later records that carry this row's new ID on forward, on the same node pair."""
        new_id = _text(row["new"])
        nfn, ntn = _text(row["nfn"]), _text(row["ntn"])
        candidates = self._select(
            lambda r: (
                _text(r["old"]) == new_id
                and (
                    not (nfn and ntn)
                    or (_text(r["otn"]) == ntn and _text(r["ofn"]) == nfn)
                )
                and r["ldf_num"] > row["ldf_num"]
                and r["status"] not in ("N", "Y")
            ),
            by_ldf_num=True,
        )
        for r in candidates:
            if _text(r["new"]) in ("", "0"):
                if r["action"] == "D":
                    related.setdefault(r["ldf_num"], r)
                else:
                    r["status"] = "Y"
            else:
                related.setdefault(r["ldf_num"], r)
                self._find_related(r, related)

    def _related_to_skip(self, sibling):
        """GetRelatedRowToSkipExtract: the next undecided record repeating this step."""
        if sibling["action"] == "M":
            id_key, from_key, to_key = "new", "nfn", "ntn"
        else:
            id_key, from_key, to_key = "old", "ofn", "otn"
        values = (
            _text(sibling[id_key]),
            _text(sibling[from_key]),
            _text(sibling[to_key]),
        )
        self._require(*values)
        return self._select(
            lambda r: (
                (_text(r[id_key]), _text(r[from_key]), _text(r[to_key])) == values
                and r["action"] == sibling["action"]
                and r["status"] not in ("N", "Y")
                and r["ldf_num"] > sibling["ldf_num"]
            ),
            by_ldf_num=True,
        )

    def _drop_through(self, related, last_ldf_num):
        """Flag related records transitory up to and including `last_ldf_num`."""
        for key in sorted(related):
            r = related[key]
            r["status"] = "N"
            if key == last_ldf_num:
                return r
            if r["action"] in ("M", "S"):
                skip = self._related_to_skip(r)
                if skip:
                    skip[0]["status"] = "N"
        return None

    def _set_transitory(self, row, related):
        """SetGenPhyTransitoryRecs."""
        if not related:
            return
        keys = sorted(related)
        merges = [k for k in keys if related[k]["action"] == "M"]
        splits = [k for k in keys if related[k]["action"] == "S"]
        if row["action"] == "A":
            to_node, from_node = _text(row["ntn"]), _text(row["nfn"])
            if to_node in ("", "0") or from_node in ("", "0"):
                row["status"] = "Y"
                return
            delete = next(
                (
                    k
                    for k in keys
                    if related[k]["action"] == "D"
                    and _text(related[k]["ofn"]) == from_node
                    and _text(related[k]["otn"]) == to_node
                ),
                None,
            )
            if delete is None:
                row["status"] = "Y"
                return
            # Added, then deleted on the same node pair: the whole lineage is transitory
            row["status"] = "N"
            for k in keys:
                related[k]["status"] = "N"
                if k == delete:
                    break
        elif row["action"] == "S":
            if (
                len(merges) == len(splits) - 1
                and merges
                and len(splits) > 1
                and merges[0] < splits[0]
                and merges[-1] < splits[-1]
            ):
                row["status"] = "Y"
                last = self._drop_through(related, splits[-2])
                if last is not None:
                    row["new_old"] = last["new"]
                self._require(_text(row["old"]), _text(row["otn"]), _text(row["ofn"]))
                repeat = self._select(
                    lambda r: (
                        _text(r["old"]) == _text(row["old"])
                        and _text(r["otn"]) == _text(row["otn"])
                        and _text(r["ofn"]) == _text(row["ofn"])
                        and r["action"] == "S"
                        and r["status"] not in ("N", "Y")
                        and r["ldf_num"] > row["ldf_num"]
                    ),
                    by_ldf_num=True,
                )
                if repeat:
                    repeat[0]["new_old"] = related[splits[-1]]["new"]
                    repeat[0]["status"] = "Y"
                    related[splits[-1]]["status"] = "N"
            else:
                row["status"] = "Y"
        elif row["action"] == "M":
            if (
                len(splits) in (len(merges) - 1, len(merges))
                and splits
                and len(merges) > 1
                and splits[0] < merges[0]
                and splits[-1] < merges[-1]
            ):
                row["status"] = "Y"
                last = self._drop_through(related, merges[-2])
                if last is not None:
                    row["new_old"] = last["new"]
                repeat = self._related_to_skip(row)
                if repeat:
                    repeat[0]["new_old"] = related[merges[-1]]["new"]
                    repeat[0]["status"] = "Y"
                    related[merges[-1]]["status"] = "N"
            else:
                row["status"] = "Y"
        else:
            row["status"] = "Y"

    def process_generic_physical(self):
        self.removed = set()
        started = False
        for row in self.rows:
            action = row["action"]
            # Records before the first add, merge or split are published as they are
            if not started and action not in ("A", "M", "S"):
                row["status"] = "Y"
                continue
            started = True
            if action in ("G", "P") and row["status"] == "":
                # A plain ID reassignment is always published
                row["status"] = "Y"
            elif row["status"] not in ("N", "Y"):
                related = {}
                self._find_related(row, related)
                self._set_transitory(row, related)

    # -- WriteToLDFExtractFile / WriteToExtractFileFromCursor ---------------------------
    def _is_deleted(self, row):
        """IsSegmentdeleted, except that G and P deletes only count on the same node pair."""
        if row["action"].strip().upper() == "D":
            return False
        new, old = _text(row["new"]).strip(), _text(row["old"]).strip()
        if self.record_type == "S":
            return (new not in ("", "0") and new in self.deleted_ids) or (
                old not in ("", "0") and old in self.deleted_ids
            )
        return (
            new not in ("", "0")
            and (self.record_type, new, frozenset((row["nfn"], row["ntn"])))
            in self.deleted_on_pair
        ) or (
            old not in ("", "0")
            and (self.record_type, old, frozenset((row["ofn"], row["otn"])))
            in self.deleted_on_pair
        )

    def published(self, swap_collapsed_ids):
        out = []
        for row in self.rows:
            if row["status"] == "N":
                continue
            if swap_collapsed_ids:
                # Prod's G/P writer swaps the two IDs on rows whose old ID was rewritten
                old, new_old = _text(row["old"]), _text(row["new_old"])
                if old and new_old and old != new_old:
                    row["new"], row["new_old"] = row["new_old"], int(old)
            if self._is_deleted(row) or row["ldf_num"] in self.removed:
                continue
            out.append(
                {
                    "record_type": self.record_type,
                    "action_code": row["action"],
                    # Prod writes zero as blank
                    "old_id": row["new_old"] or None,
                    "old_from_nodeid": row["ofn"] or None,
                    "old_to_nodeid": row["otn"] or None,
                    "new_id": row["new"] or None,
                    "new_from_nodeid": row["nfn"] or None,
                    "new_to_nodeid": row["ntn"] or None,
                }
            )
        return out


def model(dbt, session):
    dbt.config(materialized="table")

    journal = (
        dbt.ref("stg__centerlinehistory")
        .filter("release_num IS NULL AND record_type IN ('S', 'G', 'P')")
        .project(
            "change_datetime, ldf_num, record_type, action_code, old_segment_id AS old_id, "
            "new_segment_id AS new_id, old_from_nodeid, old_to_nodeid, "
            "new_from_nodeid, new_to_nodeid"
        )
        .order("change_datetime, ldf_num")
    )
    names = journal.columns
    rows = [dict(zip(names, values)) for values in journal.fetchall()]

    # Prod processes S, then G, then P; the set of deleted IDs carries across them
    f = TransitoryFilter(rows)
    f.load("S")
    f.process_segments()
    records = f.published(swap_collapsed_ids=False)
    for record_type in ("G", "P"):
        f.load(record_type)
        f.process_generic_physical()
        records += f.published(swap_collapsed_ids=True)

    df = pd.DataFrame(records, columns=COLUMNS)
    for col in COLUMNS[2:]:
        df[col] = df[col].astype("Int64")
    return df
