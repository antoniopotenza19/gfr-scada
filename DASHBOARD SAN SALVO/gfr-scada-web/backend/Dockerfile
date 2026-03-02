FROM python:3.11-slim-bookworm

WORKDIR /app

# dipendenze di sistema minime
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# requirements
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# codice (in dev viene sovrascritto dal volume, ma serve per build)
COPY . /app