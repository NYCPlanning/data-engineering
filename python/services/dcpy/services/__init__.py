"""Live services that sit on top of dcpy.lifecycle: state kept outside the stores, and the
helpers that keep it current. Nothing in dcpy.lifecycle, dcpy.connectors or dcpy.utils imports
from here; callers (apps, admin scripts, the `python -m dcpy.services` CLI) wire it in.

- `catalog`: the version catalog, which versions exist per store, in Postgres.
"""
