import streamlit as st

from .helpers import run_all_checks


def connection_checks():
    st.title("Connection Checks")
    st.markdown(
        "Live, read-only checks of each external dependency. Secret values are never shown."
    )
    if st.button("Run checks"):
        with st.spinner("Running checks..."):
            results = run_all_checks()
        n_failed = sum(not r.ok for r in results)
        if n_failed:
            st.error(f"{n_failed} of {len(results)} checks failed")
        else:
            st.success(f"All {len(results)} checks passed")
        for r in results:
            icon = "🟢" if r.ok else "🔴"
            latency = f" ({r.latency_ms:.0f} ms)" if r.latency_ms is not None else ""
            st.markdown(f"{icon} **{r.name}**{latency}: {r.detail}")
