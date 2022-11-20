FROM apache/airflow:2.4.3
USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-docker==3.2.0
RUN pip install --no-cache-dir apache-airflow-providers-apache-cassandra==3.0.0
COPY --chown=airflow:root ./dags /opt/airflow/dags