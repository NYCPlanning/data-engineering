from pathlib import Path
from dcpy.utils.s3 import upload_folder

from . import S3_BUCKET, S3_OUTPUT_DIR, BUILD_ENGINE_SCHEMA, OUTPUT_PATH, DATE

UPLOAD_PATH = Path(S3_OUTPUT_DIR) / Path(BUILD_ENGINE_SCHEMA) / Path(DATE)

if __name__ == "__main__":
    print(f"Uploading folder : {OUTPUT_PATH}\n\tto {UPLOAD_PATH} ...")
    upload_folder(
        bucket=S3_BUCKET,
        local_folder_path=OUTPUT_PATH,
        upload_path=UPLOAD_PATH,
        acl="private",
    )
