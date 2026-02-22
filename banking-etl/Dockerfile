FROM apache/airflow:2.7.2-python3.10

USER root

RUN apt-get update && apt-get install -y \
    build-essential \
    postgresql-client \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /home/airflow/airflow \
    && mkdir -p /home/airflow/airflow/logs \
    && mkdir -p /home/airflow/airflow/dags \
    && mkdir -p /home/airflow/airflow/plugins \
    && mkdir -p /home/airflow/.local \
    && chown -R 50000:0 /home/airflow \
    && chmod -R 755 /home/airflow

COPY entrypoint.sh /custom-entrypoint.sh
RUN sed -i 's/\r$//' /custom-entrypoint.sh && chmod +x /custom-entrypoint.sh

USER airflow

COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

ENV PYTHONPATH="/home/airflow/airflow:/home/airflow"

WORKDIR /home/airflow/airflow

ENTRYPOINT [ "/custom-entrypoint.sh" ]
CMD [ "webserver" ]
