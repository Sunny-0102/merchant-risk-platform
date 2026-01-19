FROM python:3.11-slim

# Create a non-root user for running the service
RUN useradd -m -s /bin/bash app

WORKDIR /workspace

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
  && rm -rf /var/lib/apt/lists/*

# Install dependencies (system-wide is fine for runtime, but devcontainer will also do --user installs)
COPY services/ingestion_api/requirements.txt /workspace/services/ingestion_api/requirements.txt
RUN pip install --no-cache-dir -r /workspace/services/ingestion_api/requirements.txt

COPY services/ingestion_api /workspace/services/ingestion_api

EXPOSE 8000

# Run as non-root at runtime too
USER app
CMD ["uvicorn", "services.ingestion_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
