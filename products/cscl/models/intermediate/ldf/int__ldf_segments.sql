{{ config(
    materialized = 'table'
) }}

/*
Prod's last check on the LDF's segment records, IsSegmentdeleted (LDFExtractHelper.cs,
CSCLEditor.dll): every record that survives transitory elimination
(int__ldf_segment_candidates) is dropped, unless it is a delete itself, when its new ID or
its journalled old ID is a deleted ID. The deleted IDs are the old ID, and the new ID when
there is one, of every delete in this release's journal (CreateTransitoryDataTable). Prod
keeps one set for the whole run and writes S, then G, then P, so an S record sees S
deletes, a G record S and G deletes, and a P record all three. Neither the node pair nor
the time of the delete matters.

For S that is right. A generic or physical ID spans many segments, though, so for G and P
it drops reassignments that LION shows took effect (CSCL-LDF-01). With the var
fix_ldf_journal_delete_scope on, a G or P record is dropped only when its ID is deleted
on the same node pair, among deletes of its own record type. Off, the default, is prod's
rule.

One row per candidate record; is_published says whether the LDF writes it.
*/

-- the order prod writes the record types in: a record sees deletes of its own type and
-- of the types written before it. Node pairs are compared either way round.
WITH candidates AS (
    SELECT
        *,
        CASE record_type WHEN 'S' THEN 1 WHEN 'G' THEN 2 WHEN 'P' THEN 3 END AS write_order,
        least(coalesce(old_from_nodeid, 0), coalesce(old_to_nodeid, 0)) AS old_node_low,
        greatest(coalesce(old_from_nodeid, 0), coalesce(old_to_nodeid, 0)) AS old_node_high,
        least(coalesce(new_from_nodeid, 0), coalesce(new_to_nodeid, 0)) AS new_node_low,
        greatest(coalesce(new_from_nodeid, 0), coalesce(new_to_nodeid, 0)) AS new_node_high
    FROM {{ ref('int__ldf_segment_candidates') }}
),

journal_deletes AS (
    SELECT
        record_type,
        CASE record_type WHEN 'S' THEN 1 WHEN 'G' THEN 2 WHEN 'P' THEN 3 END AS write_order,
        old_segment_id,
        new_segment_id,
        least(coalesce(old_from_nodeid, 0), coalesce(old_to_nodeid, 0)) AS node_low,
        greatest(coalesce(old_from_nodeid, 0), coalesce(old_to_nodeid, 0)) AS node_high
    FROM {{ ref('stg__centerlinehistory') }}
    WHERE
        release_num IS NULL
        AND record_type IN ('S', 'G', 'P')
        AND upper(trim(action_code)) = 'D'
        AND coalesce(old_segment_id, 0) <> 0
),

deleted_ids AS (
    SELECT
        write_order,
        old_segment_id AS id
    FROM journal_deletes
    UNION
    SELECT
        write_order,
        new_segment_id AS id
    FROM journal_deletes
    WHERE coalesce(new_segment_id, 0) <> 0
),

checked AS (
    SELECT
        candidates.*,
        upper(candidates.action_code) <> 'D'
        AND EXISTS (
            SELECT 1
            FROM deleted_ids
            WHERE
                deleted_ids.write_order <= candidates.write_order
                AND deleted_ids.id IN (candidates.new_id, candidates.journal_old_id)
        ) AS id_deleted_in_journal,
        upper(candidates.action_code) <> 'D'
        AND (
            (
                candidates.record_type = 'S'
                AND EXISTS (
                    SELECT 1
                    FROM deleted_ids
                    WHERE
                        deleted_ids.write_order = 1
                        AND deleted_ids.id IN (candidates.new_id, candidates.journal_old_id)
                )
            )
            OR (
                candidates.record_type <> 'S'
                AND EXISTS (
                    SELECT 1
                    FROM journal_deletes
                    WHERE
                        journal_deletes.record_type = candidates.record_type
                        AND (
                            (
                                journal_deletes.old_segment_id = candidates.new_id
                                AND journal_deletes.node_low = candidates.new_node_low
                                AND journal_deletes.node_high = candidates.new_node_high
                            )
                            OR (
                                journal_deletes.old_segment_id = candidates.journal_old_id
                                AND journal_deletes.node_low = candidates.old_node_low
                                AND journal_deletes.node_high = candidates.old_node_high
                            )
                        )
                )
            )
        ) AS id_deleted_on_node_pair
    FROM candidates
)

SELECT
    record_type,
    action_code,
    old_id,
    old_from_nodeid,
    old_to_nodeid,
    new_id,
    new_from_nodeid,
    new_to_nodeid,
    journal_old_id,
    change_datetime,
    ldf_num,
    id_deleted_in_journal,
    id_deleted_on_node_pair,
    NOT {% if var('fix_ldf_journal_delete_scope') -%}
        id_deleted_on_node_pair
    {%- else -%}
        id_deleted_in_journal
    {%- endif %} AS is_published
FROM checked
