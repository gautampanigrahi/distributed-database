FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ ./common/
COPY coordinator/ ./coordinator/
COPY shard/ ./shard/

ENV PYTHONUNBUFFERED=1
EXPOSE 8000
