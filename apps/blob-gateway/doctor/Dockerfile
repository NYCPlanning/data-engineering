# gateway-doctor: doctor.py in a small image for the ACA Job (inv-k0d.14).
#   docker build -t gateway-doctor apps/blob-gateway/doctor
#   az acr build -r <acr> -t gateway-doctor:<tag> apps/blob-gateway/doctor
FROM python:3.13-slim
RUN pip install --no-cache-dir boto3==1.43.112
COPY doctor.py /doctor/doctor.py
USER nobody
ENTRYPOINT ["python", "-u", "/doctor/doctor.py"]
