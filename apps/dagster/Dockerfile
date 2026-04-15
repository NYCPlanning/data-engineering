FROM python:3.11-slim

WORKDIR /app

# Install dagster
RUN pip install --no-cache-dir \
    dagster \
    dagster-webserver \
    dagster-postgres

# Copy dagster code
COPY . /app

# Expose webserver port
EXPOSE 3000

# Run dagster
# CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000"]
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "--path-prefix", "/dag"]
