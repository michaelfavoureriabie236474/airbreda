# Container 1: air-ingest (image name: airbreda-air)
# Build: docker build -t airbreda-air .
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common.py ingest_air.py ./
# Runs once and exits. Cron on the VM starts it every hour.
CMD ["python", "ingest_air.py"]
