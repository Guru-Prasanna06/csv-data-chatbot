# ============================================================
# Dockerfile for CSV Knowledge Graph FastAPI Backend
# Python 3.11 Slim with Non-Root User & Layer Optimization
# ============================================================

FROM python:3.11-slim-bookworm

# Prevent Python from writing .pyc files and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    APP_HOST=0.0.0.0 \
    APP_PORT=8000 \
    APP_ENV=production

# Set working directory
WORKDIR /app

# Create dedicated non-root user and group
RUN groupadd --gid 10001 appgroup && \
    useradd --uid 10001 --gid appgroup --shell /bin/bash --create-home appuser

# Install system dependencies if required and update pip
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy backend application source code
COPY --chown=appuser:appgroup app/ ./app/
COPY --chown=appuser:appgroup main.py .

# Switch to non-root user
USER appuser

# Expose FastAPI backend port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=2 \
    CMD curl -f http://localhost:8000/health || exit 0

# Entrypoint command
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
