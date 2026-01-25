import marimo

__generated_with = "0.19.2"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo


@app.cell
def _():
    mo.notebook_location()
    return


@app.cell
def _():
    repo_path = "../../../"
    local_ingest_direcrtory = repo_path + ".lifecycle/ingest/"
    return (local_ingest_direcrtory,)


@app.cell
def _(local_ingest_direcrtory):
    local_file_path = (
        local_ingest_direcrtory + "nysocfs_child_care/nysocfs_child_care.parquet"
    )
    return (local_file_path,)


@app.cell
def _(local_file_path):
    _df = mo.sql(
        f"""
        CREATE TABLE nysocfs_child_care AS SELECT * FROM read_parquet('{local_file_path}');
        SELECT * FROM nysocfs_child_care;
        """
    )
    return


@app.cell
def _(nysocfs_child_care):
    _df = mo.sql(
        f"""
        SELECT * FROM nysocfs_child_care
            where region_code = 'NYCDOH'
            and facility_status = 'License'
        """
    )
    return


@app.cell
def _(local_ingest_direcrtory):
    _df = mo.sql(
        f"""
        CREATE TABLE dohmh_child_care AS SELECT * FROM read_parquet('{local_ingest_direcrtory + "dohmh_child_care/dohmh_child_care.parquet"}');
        SELECT * FROM dohmh_child_care;
        """
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Compare
    """)
    return


@app.cell
def _(nysocfs_child_care):
    _df = mo.sql(
        f"""
        SELECT * FROM nysocfs_child_care where zip_code = '10034'
        """
    )
    return


@app.cell
def _(nysocfs_child_care):
    _df = mo.sql(
        f"""
        SELECT * FROM nysocfs_child_care where facility_id = '723858'
        """
    )
    return


@app.cell
def _(dohmh_child_care):
    _df = mo.sql(
        f"""
        SELECT * FROM dohmh_child_care where zip_code = '10034'
        """
    )
    return


@app.cell
def _(dohmh_child_care, nysocfs_child_care):
    _df = mo.sql(
        f"""
        SELECT * FROM nysocfs_child_care inner join dohmh_child_care
        on nysocfs_child_care.facility_id = dohmh_child_care.permit_number
        """
    )
    return


if __name__ == "__main__":
    app.run()
