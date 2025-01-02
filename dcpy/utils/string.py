import re


def to_camel(s: str) -> str:
    s = re.sub("[^0-9a-zA-Z]+", "_", s)
    s = re.sub("_+", "_", s)
    return "".join(x.capitalize() for x in s.lower().split("_"))


def to_snake(s: str) -> str:
    s = re.sub("[^0-9a-zA-Z]+", "_", s)
    s = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", s)
    s = re.sub("_+", "_", s)
    s = s.strip("_")
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s).lower()
