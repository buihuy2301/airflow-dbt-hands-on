import pendulum

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.utils.trigger_rule import TriggerRule

def create_dags(dag_id: str):  
    dag = DAG(
        dag_id=dag_id,
        description="Dynamic DAG generated from a list of DAGs",    
        schedule_interval='20 2-23 * * *',
        start_date=pendulum.datetime(2024, 7, 31, tz="Asia/Saigon"),
        max_active_runs=1,
        tags=['generated', 'example'],
        catchup=False,
    )
    latest_only = LatestOnlyOperator(task_id="latest_only", dag=dag)
    task1 = EmptyOperator(task_id="task1", dag=dag)
    task2 = EmptyOperator(task_id="task2", dag=dag)
    task3 = EmptyOperator(task_id="task3", dag=dag)
    task4 = EmptyOperator(task_id="task4", trigger_rule=TriggerRule.ALL_DONE,dag=dag)

    latest_only >> task1 >> [task3, task4]
    task2 >> [task3, task4]
    
    return dag
    
list_dags = ['test1','test2','test3','test4','test5']

for item in list_dags:
    dag_id = item
    globals()[dag_id] = create_dags(dag_id)
