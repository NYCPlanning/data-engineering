{{
    config(
        tags = ['de_check'],
        severity = 'warn',
        meta = {
            'description': '''
                Every (hny_id, job_number) pair should appear at most once in
                hny_corrections.csv. More than one row for a pair can silently double the
                affordable unit counts for that job.

                The DELETE in sql/_hny_match.sql removes the pair if any of its rows says
                "remove". The INSERT that follows selects FROM hny_corrections filtered
                only on whether the *pair* appears in the add list -- it never filters the
                rows it iterates by action. So a pair listed both "add" and "remove", or
                listed "add" twice, inserts once per matching row, and the duplicates are
                then summed into classa_hnyaff and all_hny_units.

                Whether it doubles anything depends on the pair reaching that INSERT: a
                pair already produced by automated matching, or whose hny_id is absent
                from hny_geo, inserts nothing. Such pairs are still worth resolving, since
                what makes them harmless can change with the source data.

                Duplicate rows also make the count(*) in the many_developments CTE treat a
                single-job HNY record as shared across jobs, mislabelling hny_jobrelate as
                many-to-many.
            ''',
            'next_steps': '''
                Warns rather than blocks: the pairs it finds need triage with the Housing
                team before anything can be resolved. Raise to error once the known ones
                are cleared at source.

                1. Fix the pair in the source workbook the Housing team maintains, not in
                   data/corrections/hny_corrections.csv -- the CSV is an export and any
                   edit to it is lost the next time the workbook is exported.
                2. An "add" contradicting a "remove" cannot be resolved by keeping both:
                   the INSERT runs after the DELETE and reinstates the pair, so the remove
                   never wins. Express "do not match these" by deleting the add row.
                3. A remove row is only needed when automated matching produces the pair
                   on its own.
            '''
        }
    )
}}

-- btrim because some rows in the corrections CSV carry a trailing space in action
SELECT
    hny_id,
    job_number,
    count(*) AS correction_rows,
    array_agg(btrim(action) ORDER BY btrim(action)) AS actions
FROM {{ source('build_sources', 'hny_corrections') }}
GROUP BY hny_id, job_number
HAVING count(*) > 1
ORDER BY hny_id, job_number
