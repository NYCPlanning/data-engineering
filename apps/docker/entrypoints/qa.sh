#!/bin/bash
# QA streamlit app. Run from apps/qa so its .streamlit/config.toml applies.
set -e

cd /app/data-engineering/apps/qa
exec streamlit run src/index.py \
    --server.port=8501 \
    --server.address=0.0.0.0 \
    --server.baseUrlPath=/qaqc
