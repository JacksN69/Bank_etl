#!/bin/bash
set -e

# ============================================================================
# Banking ETL Pipeline - Project Root Docker Entrypoint
# ============================================================================

echo "Starting Airflow Entrypoint..."
echo "Current user: $(id)"
echo "AIRFLOW_HOME: ${AIRFLOW_HOME}"

# Create necessary directories (avoid chown/chmod on mounted paths)
mkdir -p "${AIRFLOW_HOME}/logs" "${AIRFLOW_HOME}/dags" "${AIRFLOW_HOME}/plugins" || true

# Initialize Airflow database
echo "Initializing Airflow database..."
airflow db migrate || true

# Create default admin user if it doesn't exist
echo "Ensuring admin user..."
airflow users create \
    --username ${_AIRFLOW_WWW_USER_USERNAME:-airflow} \
    --password ${_AIRFLOW_WWW_USER_PASSWORD:-airflow} \
    --firstname Airflow \
    --lastname Admin \
    --role Admin \
    --email admin@example.com || true

echo "Entrypoint initialization complete. Executing: $@"
# Map short commands used by docker-compose to full airflow commands.
if [ "$#" -gt 0 ]; then
  case "$1" in
    webserver|scheduler|triggerer|dag-processor)
      exec airflow "$@"
      ;;
    *)
      exec "$@"
      ;;
  esac
fi

exec airflow webserver
