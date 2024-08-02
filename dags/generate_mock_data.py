import os
from setup.generate_mock_data import generate_mock_data_with_create_date
from setup.ETL import Pipeline
from  datetime import timedelta
import logging

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

home = os.environ["HOME"]
local_data_dir = f"{home}/data"
table_names = ["customers", "loans"]
SQL_DIR = "/usr/app/setup"

# Connect to PostgreSQL
postgress_config = {
    "database": os.environ["POSTGRES_DB_DBT"],
    "user": os.environ["POSTGRES_USER_DBT"],
    "password": os.environ["POSTGRES_PASSWORD_DBT"],
    "host": os.environ["POSTGRES_HOST"],
    "port": os.environ["POSTGRES_PORT"],
    "options": "-c search_path=dbo," + os.environ["POSTGRES_DB_SCHEMA"]
}

def _generate_mock_data(ts):
    if not os.path.exists(local_data_dir):
        os.mkdir(local_data_dir)
    generate_mock_data_with_create_date(save_path=local_data_dir, create_at=ts)
    logging.info("Generated data at: %s", ts)

def _insert_to_posgres():
    pipeline = Pipeline()
    for table_name in table_names:
        if not os.path.exists(f"{local_data_dir}/{table_name}_new.parquet"):
            raise FileNotFoundError(
                f"File {local_data_dir}/{table_name}.parquet not found"
            )
        pipeline = Pipeline()
        try:
            pipeline.local_to_postgres_transfer(
                local_path=f"{local_data_dir}/{table_name}_new.parquet",
                config=postgress_config,
                table_name=table_name,
            )
            logging.info("ETL pipeline completed for table: %s", table_name)
        except Exception as e:  # pylint: disable=broad-except
            logging.error("Error in ETL pipeline for table %s: %s", table_name, str(e))

with DAG(
    dag_id="generate_mock_data",
    schedule_interval = timedelta(minutes=5),
    start_date=pendulum.datetime(2024, 7, 31, tz="Asia/Saigon"),
    catchup=False,
    tags=["demo"],
) as dag:
    generate_mock_data = PythonOperator(
        task_id="generate_mock_data",
        python_callable=_generate_mock_data,
    )
    insert_to_staging = PythonOperator(
        task_id="insert_to_staging",
        python_callable=_insert_to_posgres
    )