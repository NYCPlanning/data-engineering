# syntax=docker/dockerfile:1

# Stage 1: Clone product metadata repo
FROM alpine/git:latest as metadata-cloner
RUN git clone --depth 1 https://github.com/NYCPlanning/product-metadata.git /product-metadata

# Stage 2: Main application
FROM nycplanning/base:latest

WORKDIR /app

# Copy dependency files first for better caching
COPY --link admin/run_environment/constraints.txt admin/run_environment/
COPY --link admin/run_environment/requirements.txt admin/run_environment/
COPY --link apps/notebook-server/requirements.txt apps/notebook-server/

# Install dependencies (these layers will be cached unless requirements change)
RUN python3 -m pip install -r admin/run_environment/requirements.txt -c admin/run_environment/constraints.txt
RUN python3 -m pip install -r apps/notebook-server/requirements.txt -c admin/run_environment/constraints.txt

# Copy package files and install the main package
COPY --link pyproject.toml .
COPY --link dcpy/ dcpy/
RUN python3 -m pip install . -c admin/run_environment/constraints.txt

# Copy product metadata from stage 1
COPY --from=metadata-cloner /product-metadata/ product-metadata/

# Copy application files
COPY --link notebooks/ notebooks/
COPY --link apps/notebook-server/ apps/notebook-server/

RUN rm -rf dcpy

EXPOSE 8080

HEALTHCHECK CMD curl --fail http://localhost:8080/ || exit 1

ENTRYPOINT ["python3", "apps/notebook-server/server.py"]
