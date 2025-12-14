# Use Python 3.9 slim image as base
FROM python:3.9-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ .

# Set environment variables from base-config
ENV SERVICE_NAME=${SERVICE_NAME} \
    STAGE=${STAGE:-dev} \
    REGION=${REGION:-us-east-1} \
    LOG_LEVEL=${LOG_LEVEL:-INFO}

# Expose port
EXPOSE 3000

# Run the application
CMD ["python", "main.py"] 