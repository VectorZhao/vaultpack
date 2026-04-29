FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    BACKUP_DATA_DIR=/data \
    BACKUP_SOURCE_ROOT=/backup-source

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

RUN mkdir -p /data /backup-source /tmp/backup-work

EXPOSE 8080

CMD ["sh", "-c", "if [ \"$VAULTPACK_ROLE\" = \"agent\" ]; then python -m app.agent; else gunicorn --bind 0.0.0.0:8080 --workers 1 --threads 4 --timeout 3600 'app.main:create_app()'; fi"]
