FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/

RUN groupadd -g 10001 loaderuser && \
    useradd -u 10001 -g loaderuser -s /bin/sh loaderuser && \
    chown -R loaderuser:loaderuser /app

USER 10001:10001

CMD ["python", "-m", "app.main"]
