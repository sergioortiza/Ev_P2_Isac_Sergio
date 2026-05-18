FROM apache/airflow:3.2.1

# Instalamos los conectores de base de datos de forma permanente
USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-mysql \
    apache-airflow-providers-common-sql