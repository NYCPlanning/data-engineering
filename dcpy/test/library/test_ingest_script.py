import os

from dcpy.library.ingest import Ingestor

from . import template_path


def test_bpl_libraries():
    ingestor = Ingestor()
    ingestor.csv(f"{template_path}/bpl_libraries.yml", version="test")
    assert os.path.isfile(".library/datasets/bpl_libraries/test/bpl_libraries.csv")
