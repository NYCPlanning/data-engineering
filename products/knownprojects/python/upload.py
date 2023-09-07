import os
import glob
import sys
from pathlib import Path

from dcpy.utils.s3 import upload_folder

from . import S3_BUCKET, OUTPUT_DIR, S3_OUTPUT_DIR


if __name__ == "__main__":
    print(f"Uploading folder : {OUTPUT_DIR}\n\tto {S3_OUTPUT_DIR} ...")
    upload_folder(
        bucket=S3_BUCKET,
        local_folder_path=OUTPUT_DIR,
        upload_path=Path(S3_OUTPUT_DIR),
        acl="private",
    )
