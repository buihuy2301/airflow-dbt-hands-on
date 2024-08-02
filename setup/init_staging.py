"""
This script is used to run the DDL.sql file to create the tables in the PostgreSQL database.
"""

import os
import logging
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from generate_mock_data import generate_mock_data
from ETL import Pipeline


def main():
    """_summary_"""
    home = os.environ["HOME"]
    local_data_dir = f"{home}/data"
    table_names = ["customers", "loans"]
    sql_dir = "/usr/app/setup"

    # Generate mock data for the Customers and Loans tables
    # Check if the data directory exists
    if not os.path.exists(local_data_dir):
        os.mkdir(local_data_dir)
    generate_mock_data(save_path=local_data_dir)
    logging.info("Generated data for Customers and Loans tables at: %s", local_data_dir)

    # Connect to PostgreSQL
    postgress_config = {
        "database":  os.environ["POSTGRES_DB"],
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "host": os.environ["POSTGRES_HOST"],
        "port": os.environ["POSTGRES_PORT"],
        "options": "-c search_path=dbo," + os.environ["POSTGRES_DB_SCHEMA"]
    }

    # Create a cursor object
    conn = psycopg2.connect(**postgress_config)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    with open(f"{sql_dir}/DDL.sql", "r", encoding="UTF-8") as f:
        sql_queries = f.read()

    cur.execute(sql_queries)

    # Close the cursor and connection
    cur.close()
    conn.close()

    # ETL pipeline for mock data
    for table_name in table_names:
        if not os.path.exists(f"{local_data_dir}/{table_name}.parquet"):
            raise FileNotFoundError(
                f"File {local_data_dir}/{table_name}.parquet not found"
            )
        pipeline = Pipeline()
        try:
            pipeline.local_to_postgres_transfer(
                local_path=f"{local_data_dir}/{table_name}.parquet",
                config=postgress_config,
                table_name=table_name,
            )
            logging.info("ETL pipeline completed for table: %s", table_name)
        except Exception as e:  # pylint: disable=broad-except
            logging.error("Error in ETL pipeline for table %s: %s", table_name, str(e))


if __name__ == "__main__":
    main()
