from pandera import extensions


@extensions.register_check_method(check_type="element_wise")
def is_geom_point(s):
    try:
        return s.geom_type == "Point"
    except ValueError:
        return False
