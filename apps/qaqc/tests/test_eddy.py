from src.edde.helpers import compare_header

def test_compare_header():
    assert(compare_header("column1", "column1"))
    assert(compare_header("column1", "column2"))
    assert(not compare_header("column1", "other_column2"))
    assert(not compare_header("column1", "bolumn1"))