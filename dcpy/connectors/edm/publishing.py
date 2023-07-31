from datetime import date
from pathlib import Path

from dcpy.connectors import s3
from dcpy.git import git_branch

BUCKET = "edm-publishing"


def upload(
    output: Path,
    publishing_folder: str,
    acl: str,
    *,
    s3_subpath: str = None,
    latest: bool = True,
    include_foldername: bool = False,
    max_files: int = 20
):
    if s3_subpath is None:
        ## potential TODO, change this to environment variable
        s3_subpath = git_branch()
    prefix = Path(publishing_folder) / s3_subpath
    version_folder = prefix / str(date.today())
    key = version_folder / output.name
    if output.is_dir():
        s3.upload_folder(
            BUCKET,
            output,
            version_folder / key,
            acl,
            include_foldername=include_foldername,
            max_files=max_files,
        )
        if latest:
            ## much faster than uploading again
            s3.copy_folder(BUCKET, version_folder, prefix / "latest", acl, max_files)
    else:
        s3.upload_file("edm-publishing", output, str(key), "public-read")
        if latest:
            ## much faster than uploading again
            s3.copy_file(BUCKET, str(key), str(prefix / "latest" / output.name), acl)
