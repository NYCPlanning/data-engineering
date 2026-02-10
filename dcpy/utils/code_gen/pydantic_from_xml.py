from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path
from typing import DefaultDict, Dict, List

import typer
from typing_extensions import TypedDict


def _to_class_name(tag: str) -> str:
    # Remove namespace if present
    tag = tag.split("}", 1)[-1]
    # camel-case / pascal-case
    parts = re.split("[^0-9a-zA-Z]+", tag)
    return "".join(p.capitalize() or "_" for p in parts)


def _to_field_name(name: str) -> str:
    # snake_case version of name (preserve case for acronyms somewhat)
    name = name.split("}", 1)[-1]
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    s2 = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1)
    s3 = re.sub("[^0-9a-zA-Z_]+", "_", s2)
    return s3.lower().strip("_") or "field"


def generate_from_xml(xml_path: str | Path, out_path: str | Path):
    """Generate pydantic-xml classes from a sample XML file.

    This is a best-effort generator intended for iterative development.
    It inspects the provided XML file and writes a Python module with
    class definitions using pydantic-xml patterns (BaseXmlModel, attr, element).
    """
    xml_path = Path(xml_path)
    out_path = Path(out_path)
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Typed container for tag metadata
    class TagInfo(TypedDict):
        attrs: List[str]
        children: List[str]
        text: bool
        samples: List[str]
        attr_samples: Dict[str, List[str]]

    def _new_taginfo() -> TagInfo:
        return {
            "attrs": [],
            "children": [],
            "text": False,
            "samples": [],
            "attr_samples": {},
        }

    # Collect info per tag
    tags: DefaultDict[str, TagInfo] = defaultdict(_new_taginfo)

    def visit(elem: ET.Element):
        tag = elem.tag
        entry = tags[tag]
        # record attributes in order seen
        for k, v in elem.attrib.items():
            if k not in entry["attrs"]:
                entry["attrs"].append(k)
            # record sample values for this attribute
            entry["attr_samples"].setdefault(k, []).append((v or "").strip())
        # text
        txt = (elem.text or "").strip()
        if txt:
            entry["text"] = True
            entry["samples"].append(txt)
        # children
        for child in elem:
            child_tag = child.tag
            entry["children"].append(child_tag)
            visit(child)

    visit(root)

    # detect repeated children to decide lists
    def _new_int_defaultdict() -> DefaultDict[str, int]:
        return defaultdict(int)

    children_counts: DefaultDict[str, DefaultDict[str, int]] = defaultdict(
        _new_int_defaultdict
    )
    for tag, info in tags.items():
        for ch in info["children"]:
            children_counts[tag][ch] += 1

    # Identify leaf text-only tags; these will be mapped to primitive fields
    leaf_text_tags = {
        t
        for t, info in tags.items()
        if info["text"] and not info["attrs"] and not info["children"]
    }

    def _infer_text_type(samples: List[str]) -> str:
        """Return 'int'|'float'|'str' based on samples.

        Heuristics:
        - If every sample is an integer, return 'int'.
        - If any sample is a float AND can be round-tripped exactly, return 'float'.
        - Otherwise return 'str' (to preserve precision).

        Edge case: preserve leading-zero numeric-looking strings as strings.
        Examples preserved as strings: "00", "01", "0000", "-01".
        However "0.1", "1e3", "-0.5" are treated as numeric where appropriate.
        """
        if not samples:
            return "str"
        all_int = True
        any_float = False

        def _has_leading_zero_integer_part(s: str) -> bool:
            # strip leading sign
            if s.startswith(("+", "-")):
                s2 = s[1:]
            else:
                s2 = s
            # for floats in scientific notation (1e3) allow parsing below; handle split at e/E
            s_no_exp = s2.split("e", 1)[0].split("E", 1)[0]
            # if decimal, consider integer part only
            int_part = s_no_exp.split(".", 1)[0]
            return len(int_part) > 1 and int_part.startswith("0")

        def _can_round_trip_as_float(s: str) -> bool:
            """Check if a string can be converted to float and back without precision loss."""
            try:
                f = float(s)
                return str(f) == s
            except Exception:
                return False

        for s in samples:
            s = s.strip()
            if s == "":
                return "str"

            # If integer-looking but with leading zeros, treat as string.
            # e.g. "01", "000" should remain strings; but "0" is numeric.
            if _has_leading_zero_integer_part(s):
                return "str"

            # try int
            try:
                # int() will accept things like '+1' and '-2'
                int(s)
                continue
            except Exception:
                all_int = False

            # try float (accepts scientific notation)
            try:
                float(s)
                # Check if it can round-trip without precision loss
                if not _can_round_trip_as_float(s):
                    return "str"
                any_float = True
            except Exception:
                return "str"

        if all_int:
            return "int"
        if any_float:
            return "float"
        return "str"

    # Order classes: emit child classes before parent classes using a
    # topological sort (Kahn's algorithm). We build a graph where an edge
    # child -> parent ensures children are emitted first. We only include
    # tags that will be emitted as classes (i.e., non-leaf text-only tags).
    emit_tags = [t for t in tags.keys() if t not in leaf_text_tags]

    # build adjacency list for child -> parent edges
    adj: DefaultDict[str, List[str]] = defaultdict(list)
    indegree: DefaultDict[str, int] = defaultdict(int)

    for parent in emit_tags:
        for ch in set(tags[parent]["children"]):
            if ch in leaf_text_tags:
                continue
            # child -> parent
            adj[ch].append(parent)
            indegree[parent] += 1

    # nodes with zero indegree (no children pointing to them) come first
    zero = sorted([n for n in emit_tags if indegree.get(n, 0) == 0])
    topo: List[str] = []
    while zero:
        n = zero.pop(0)
        topo.append(n)
        for m in adj.get(n, []):
            indegree[m] -= 1
            if indegree[m] == 0:
                zero.append(m)
        zero.sort()

    if len(topo) != len(emit_tags):
        # cycle detected — this generator assumes no reused/cyclic class
        # references. Raise a clear error for now.
        raise RuntimeError(
            "Cycle detected in tag dependencies — cannot emit classes without "
            "forward references. Consider simplifying the XML or enabling "
            "quoted forward-references."
        )

    # ensure non-emitted (leaf text-only) tags are not included
    sorted_tags = topo
    root_tag = root.tag
    if root_tag in sorted_tags:
        sorted_tags = [t for t in sorted_tags if t != root_tag] + [root_tag]

    lines = []
    # add a small autogenerated file header including the source XML filename
    lines.append(
        f'"""Autogenerated by generate_pydantic_xml_classes.py\n'
        f"Source XML: {xml_path.name}\n"
        f'Do not edit directly.\n"""'
    )
    # lines.append("from __future__ import annotations")
    lines.append("from pydantic_xml import BaseXmlModel, element, attr")
    lines.append("")

    for tag in sorted_tags:
        # Skip emitting classes for leaf text-only tags; they'll be inlined as primitives
        if tag in leaf_text_tags:
            continue

        info = tags[tag]
        class_name = _to_class_name(tag)
        class_lines = [f'class {class_name}(BaseXmlModel, tag="{tag}"):']

        # attributes first
        for a in info["attrs"]:
            fname = _to_field_name(a)
            # infer attribute type from observed samples
            atyp = _infer_text_type(info["attr_samples"].get(a, []))
            class_lines.append(
                f'    {fname}: {atyp} | None = attr(name="{a}", default=None)'
            )

        # children
        if info["children"]:
            seen = []
            for ch in info["children"]:
                if ch in seen:
                    continue
                seen.append(ch)
                fname = _to_field_name(ch)
                count = children_counts[tag].get(ch, 1)

                if ch in leaf_text_tags:
                    # inline as primitive text field
                    typ = _infer_text_type(tags[ch]["samples"])
                    if count > 1:
                        class_lines.append(
                            f'    {fname}: list[{typ}] | None = element(tag="{ch}", text=True, default_factory=list)'
                        )
                    else:
                        class_lines.append(
                            f'    {fname}: {typ} | None = element(tag="{ch}", text=True, default=None)'
                        )
                else:
                    ch_class = _to_class_name(ch)
                    # Use quoted forward-references for generated classes to
                    # avoid import-time ordering issues (pydantic-xml registers
                    # serializers at class creation). Quoting the annotation
                    # defers evaluation and prevents "partially initialized" errors.
                    if count > 1:
                        type_str = f"list[{ch_class}] | None"
                        class_lines.append(
                            f'    {fname}: "{type_str}" = element(tag="{ch}", default_factory=list)'
                        )
                    else:
                        type_str = f"{ch_class} | None"
                        class_lines.append(
                            f'    {fname}: "{type_str}" = element(tag="{ch}", default=None)'
                        )

        # If the element has text (even when it has attributes) but no child elements,
        # represent that text as a `value` field. This captures cases like
        # <mdDateSt Sync="TRUE">19261122</mdDateSt> where text and attributes coexist.
        if (not info["children"]) and info["text"]:
            val_type = _infer_text_type(info["samples"])
            # Bind the element's own text to a primitive-typed field per pydantic-xml
            # docs: a primitive-typed field on the model maps to the element's text.
            # Primitive types don't need quoting.
            class_lines.append(f"    value: {val_type} | None = None")

        # if class body empty, add pass
        if len(class_lines) == 1:
            class_lines.append("    pass")

        lines.extend(class_lines)
        lines.append("")
    if root.tag not in tags:
        # shouldn't happen
        pass

    out_text = "\n".join(lines)
    out_path.write_text(out_text, encoding="utf-8")


app = typer.Typer()


@app.command()
def main(
    xml: Path = typer.Argument(..., help="Input XML file"),
    out: Path | None = typer.Option(
        None,
        "-o",
        "--out",
        help=(
            "Output Python file. If omitted, defaults to the XML filename with a .py suffix."
        ),
    ),
) -> None:
    """Generate pydantic-xml classes from a sample XML file.

    Example: python -m experiment.generate_pydantic_xml_classes sample.xml -o generated.py
    """
    if out is None:
        out = xml.with_suffix(".py")
    generate_from_xml(xml, out)
    typer.echo(f"Wrote classes to: {out}")


if __name__ == "__main__":
    app()
