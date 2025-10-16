FROM apache/airflow:2.9.1
USER root
RUN pip install pyspark==3.5.1
USER airflow

