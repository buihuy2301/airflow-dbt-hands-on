FROM apache/airflow:2.9.2
COPY airflow_requirement.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /airflow_requirement.txt