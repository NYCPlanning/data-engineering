"""Move the per-file diff rows into validation_output/diff_rows.zip.

Run last in the QA stage, once every step that reads the loose files (summarize_diffs)
has run. The files are the ones diffs_summary.csv lists; the QA app's Diff Row Viewer
reads their members from the zip. Summary csvs stay loose.
"""

import csv
import zipfile

from paths import validation_dir


def main() -> None:
    out_dir = validation_dir()
    with (out_dir / "diffs_summary.csv").open(newline="") as f:
        names = [row["File name"] for row in csv.DictReader(f)]
    files = [out_dir / n for n in names if (out_dir / n).is_file()]
    with zipfile.ZipFile(out_dir / "diff_rows.zip", "w", zipfile.ZIP_DEFLATED) as zf:
        for path in files:
            zf.write(path, path.name)
    for path in files:
        path.unlink()
    print(f"Zipped {len(files)} diff row files into diff_rows.zip")


if __name__ == "__main__":
    main()
