FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ ./common/
COPY coordinator/ ./coordinator/
COPY shard/ ./shard/

# WAL files live here. docker-compose mounts a named volume per service
# so data survives container restarts and we can demonstrate recovery.
RUN mkdir -p /data
VOLUME ["/data"]

ENV PYTHONUNBUFFERED=1
EXPOSE 8000
