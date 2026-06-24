# syntax=docker/dockerfile:1

FROM nycplanning/base:latest

WORKDIR /app

# Copy dependency files first for better caching
COPY --link admin/run_environment/constraints.txt admin/run_environment/
COPY --link admin/run_environment/requirements.txt admin/run_environment/
COPY --link apps/notebook-server/requirements.txt apps/notebook-server/

# Install dependencies
RUN python3 -m pip install -r admin/run_environment/requirements.txt -c admin/run_environment/constraints.txt
RUN python3 -m pip install -r apps/notebook-server/requirements.txt -c admin/run_environment/constraints.txt

# Copy package files and install the main package
COPY --link pyproject.toml .
COPY --link dcpy/ dcpy/
COPY --link product-metadata/ product-metadata/
RUN python3 -m pip install . -c admin/run_environment/constraints.txt

# Copy application files
COPY --link notebooks/ notebooks/
COPY --link apps/notebook-server/ apps/notebook-server/

RUN rm -rf dcpy

EXPOSE 8080

HEALTHCHECK CMD curl --fail http://localhost:8080/ || exit 1

ENTRYPOINT ["python3", "apps/notebook-server/server.py"]
