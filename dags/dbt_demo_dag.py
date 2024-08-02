import os
from datetime import timedelta
import logging
import pendulum

from airflow.decorators import task_group
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import DagBag, TaskInstance

from utils.dbt_utils import dbt_docker_run

args={
    'owner' : 'huybq19',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'start_date': pendulum.datetime(2024, 7, 31, tz="Asia/Saigon"),
}




LOGGER = logging.getLogger("airflow.test_dag")
HOME = os.environ["HOME"]
DBT_ROOT_PATH = "/Users/huybq/Documents/dev-dir/dbt/dbt_demo"
TARGET_MOUTH_PATH = "/usr/app"
MOUNT_TYPE = "bind"
MANIFEST_PATH = "/opt/airflow/dags/dbt/target/manifest.json"
DBT_IMAGE = "dbt-dbt:latest"
DOCKER_URL = "TCP://docker-socket-proxy:2375"
NETWORK_MODE = "container:dbt-postgres-1"


def determine_branch(**kwargs):
    ti = kwargs["ti"]
    for task in kwargs["dag"].task_ids:
        if ti.xcom_pull(task_ids=task):
            if "ERROR=1" in ti.xcom_pull(task_ids=task).split(" "):
                return "send_slack"
    return "success"


with DAG(
    dag_id="dbt_demo",
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    tags=["dbt"],
    default_args=args,
) as dag:
    start_task = EmptyOperator(task_id="start_task", dag=dag)

    new_data_sensor = ExternalTaskSensor(
        task_id = 'new_data_sensor',
        external_dag_id = 'generate_mock_data',
        external_task_id = 'insert_to_staging',
        poke_interval = 10,
        mode="reschedule",
        timeout = 300,
    )

    @task_group(group_id="dbt_tasks")
    def dbt_tasks():
        """_summary_"""
        dbt_docker_tasks, nodes = dbt_docker_run(
            dag=dag,
            dbt_image=DBT_IMAGE,
            dbt_host_path=DBT_ROOT_PATH,
            target_mouth_path=TARGET_MOUTH_PATH,
            mount_type=MOUNT_TYPE,
            manifest_path=MANIFEST_PATH,
            docker_url=DOCKER_URL,
            network_mode=NETWORK_MODE,
        )
        for node_id, node_info in nodes.items():
            if upstream_nodes := node_info["depends_on"].get("nodes"):
                for upstream_node in upstream_nodes:
                    if not upstream_node.startswith("source"):
                        dbt_docker_tasks.get(upstream_node) >> dbt_docker_tasks.get(
                            node_id
                        )
            else:
                dbt_docker_tasks.get(node_id)

    # Branching decision
    branch_task = BranchPythonOperator(
        task_id="branch_task",
        python_callable=determine_branch,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag,
    )

    success_branch = EmptyOperator(
        task_id="success", trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag
    )
    failure_branch = EmptyOperator(task_id="send_slack", dag=dag)

    [start_task, new_data_sensor] >> dbt_tasks() >> branch_task
    branch_task >> success_branch
    branch_task >> failure_branch >> success_branch
