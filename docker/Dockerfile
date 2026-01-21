FROM apache/airflow:2.7.3-python3.10

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root src/ /opt/airflow/src/
COPY --chown=airflow:root config/ /opt/airflow/config/

ENV PYTHONPATH="/opt/airflow:${PYTHONPATH}"
