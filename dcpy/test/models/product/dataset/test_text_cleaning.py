import dcpy.product_metadata.models.metadata.product as md


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
    assert md.normalize_text(messy_description) == clean_description
