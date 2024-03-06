from dcpy.extract import TMP_DIR


def runner():
    path = TMP_DIR / "t.txt"
    path.touch()
    return path
