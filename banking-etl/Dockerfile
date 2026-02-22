# ============================================================================
# Banking ETL Pipeline - Dockerfile for Custom Image
# ============================================================================
# Optional: Build custom Airflow image with ETL requirements

FROM apache/airflow:2.7.2-python3.10

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    postgresql-client \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Pre-create directories with airflow ownership (before switching users)
RUN mkdir -p /home/airflow/airflow \
    && mkdir -p /home/airflow/airflow/logs \
    && mkdir -p /home/airflow/airflow/dags \
    && mkdir -p /home/airflow/airflow/plugins \
    && mkdir -p /home/airflow/.local \
    && chown -R 50000:0 /home/airflow \
    && chmod -R 755 /home/airflow

# Copy and setup custom entrypoint script
COPY entrypoint.sh /custom-entrypoint.sh
RUN sed -i 's/\r$//' /custom-entrypoint.sh && chmod +x /custom-entrypoint.sh

USER airflow

# Copy requirements and install Python packages
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Set environment
ENV PYTHONPATH="/home/airflow/airflow:/home/airflow"

WORKDIR /home/airflow/airflow

# Use custom entrypoint
ENTRYPOINT [ "/custom-entrypoint.sh" ]
CMD [ "webserver" ]
