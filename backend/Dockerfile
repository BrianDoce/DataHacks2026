FROM python:3.11-slim

WORKDIR /backend

# Install OS-level dependencies needed by some Python packages (e.g. cryptography)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        libffi-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/

# Expose the uvicorn port
EXPOSE 8000

# Run as non-root for security
RUN adduser --disabled-password --gecos "" appuser
USER appuser

# DATABRICKS_HOST, DATABRICKS_HTTP_PATH, and DATABRICKS_TOKEN must be
# supplied at runtime — never baked into the image.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
