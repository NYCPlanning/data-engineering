FROM nycplanning/base:latest

WORKDIR /app

WORKDIR /app

# Copy dependency files first for better caching
COPY admin/run_environment/constraints.txt admin/run_environment/
COPY admin/run_environment/requirements.txt admin/run_environment/
COPY apps/notebook-server/requirements.txt apps/notebook-server/

# Install dependencies (these layers will be cached unless requirements change)
RUN python3 -m pip install -r admin/run_environment/requirements.txt -c admin/run_environment/constraints.txt
RUN python3 -m pip install -r apps/notebook-server/requirements.txt -c admin/run_environment/constraints.txt

# Copy package files and install the main package
COPY pyproject.toml .
COPY dcpy/ dcpy/
RUN python3 -m pip install . -c admin/run_environment/constraints.txt

# Copy application files
COPY notebooks/ notebooks/
COPY apps/notebook-server/ apps/notebook-server/

RUN rm -rf dcpy

EXPOSE 8080

HEALTHCHECK CMD curl --fail http://localhost:8080/ || exit 1

ENTRYPOINT ["python3", "apps/notebook-server/server.py"]
