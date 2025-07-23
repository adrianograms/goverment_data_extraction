from airflow.decorators import dag,task
from tasks.operations_task import * 

@dag(
    dag_id="dag_teste",
    start_date=datetime.now(),
    schedule=None,
    catchup=False
)
def dag_test_group_task(bulk_size):
    crud_project = crud_projeto_investimento(bulk_size)
    crud_project

dag_test_group_task = dag_test_group_task(500)