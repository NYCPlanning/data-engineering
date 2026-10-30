# One slim, arch-native python image for every python service in the devstack: dagster
# webserver + daemon, dagster-broker and the mock workers (tracker inv-k0d.10). The repo is
# mounted at /repo at runtime and put on PYTHONPATH, so code changes need only a restart.
# Deliberately not apps/dagster's image: that one is amd64-only and carries dcpy + GDAL.
FROM python:3.13-slim

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/repo/apps/dagster-broker:/repo/apps/devstack/demo_location
WORKDIR /repo
