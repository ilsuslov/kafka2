FROM python:3.10-slim

# Для компиляции python-rocksdb нужны системные заголовки
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential librocksdb-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .

CMD ["faust", "-A", "app", "worker", "-l", "info"]
