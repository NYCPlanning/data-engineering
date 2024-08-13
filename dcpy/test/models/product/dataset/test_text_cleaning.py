import dcpy.models.product.dataset.metadata_v2 as md_v2


def test_description_cleaning():
    messy_description = """

    Newlines Above (Should be removed)
    Weird Characters: Apostrophe’s big–dash “lquote rquote”

    Two Newlines Above (Should be Preserved)
    Newline below (Should be removed)
    """

    clean_description = """Newlines Above (Should be removed)
    Weird Characters: Apostrophe's big-dash "lquote rquote"

    Two Newlines Above (Should be Preserved)
    Newline below (Should be removed)"""
    assert md_v2.normalize_text(messy_description) == clean_description
