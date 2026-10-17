from dcpy.lifecycle.builds.qa import run_qa


def test_qa_commands_see_stage_dirs(tmp_path, monkeypatch):
    for name in ("BUILD", "PACKAGE", "QA"):
        monkeypatch.setenv(f"{name}_ENV_OUTPUT_DIR", str(tmp_path / name.lower()))
    lock = tmp_path / "recipe.lock.yml"
    lock.write_text(
        """
name: t
product: t
version: v1
inputs:
  datasets: []
stage_config:
  builds.qa:
    commands:
    - name: record_dirs
      run: echo "$BUILD_DIR|$PACKAGE_DIR|$QA_DIR" > "$QA_DIR/dirs.txt"
"""
    )
    qa_dir = run_qa(lock, product_path=tmp_path)
    assert qa_dir == tmp_path / "qa"
    assert (qa_dir / "dirs.txt").read_text().strip() == "|".join(
        str(tmp_path / d) for d in ("build", "package", "qa")
    )
