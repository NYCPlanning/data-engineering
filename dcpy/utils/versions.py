import re


def parse(v: str) -> dict[str, str]:
    """Parse a version from our standard format, e.g '23v2.11'."""
    # TODO: implement other version types.
    reg = re.compile(r"(\d{2})v\d.*")
    if reg.match(v) is not None:
        year, rst = v.split("v")
        rst_split = rst.split(".")
        version = {"year": year}
        if len(rst_split) > 0:
            version["major"] = rst_split[0]
        if len(rst_split) > 1:
            version["minor"] = rst_split[1]
        return version
    else:
        raise Exception(
            f"Tried to parse version {v} but it did not match the expected format"
        )


def bump(v: str, bumped_part: str) -> str:
    """Bump a specific part of a version. e.g major, minor or year."""
    parsed = parse(v)
    to_bump = int(parsed.get(bumped_part, 0))
    parsed[bumped_part] = str(to_bump + 1)
    if bumped_part == "major" and "minor" in parsed:
        del parsed["minor"]
    return (
        parsed["year"]
        + "v"
        + parsed.get("major", "")
        + ("." + parsed["minor"] if "minor" in parsed else "")
    )
