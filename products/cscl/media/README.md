# media/

Images referenced from generated-doc content (`docs/boilerplate/*.yml`, or injected by
`scripts/generate_docs.py`), organized in subfolders by topic (e.g. `media/rpl/`).

Convention: an `Image.path` (in a boilerplate yml, or set in code) is always written
relative to the product root (`products/cscl/`) - e.g. `media/rpl/my_diagram.png`, not
`../media/rpl/my_diagram.png` or an absolute path. Whatever writes the final rendered
file is responsible for resolving that path relative to wherever the file actually
lands, since generated output isn't guaranteed to sit at the product root itself.

An image that's *already* referenced by another checked-in doc (`design_doc.md`,
`README.md`, ...) should stay where it is rather than being duplicated into `media/` -
the same product-root-relative convention applies to it either way. `media/` is for
images that exist specifically for generated documentation.
