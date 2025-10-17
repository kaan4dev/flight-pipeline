FROM apache/airflow:2.9.1

USER root
COPY airflow/requirements.txt /requirements.txt
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt
