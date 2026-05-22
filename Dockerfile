FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements/api.txt requirements/api.txt
RUN pip install --no-cache-dir -r requirements/api.txt

COPY . .

EXPOSE 8000
