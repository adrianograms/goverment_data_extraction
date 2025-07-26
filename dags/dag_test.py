from airflow.decorators import dag,task
from tasks.operations_task import * 
from dotenv import load_dotenv

load_dotenv()
page_size = 100
url_base = "https://api.obrasgov.gestao.gov.br"
endpoint = "/obrasgov/api/execucao-financeira"
current_year = datetime.now().year
initial_year = int(current_year)
final_year = int(current_year)
dest_path = '/home/adriano/Documentos/airflow/database/dest/bronze/execucao-financeira'
errors_limit = - 1
errors_consecutives_limit = 5
executions_limit = -1
days = 30
bulk_size = 500
ufs = ['PR']  

@dag(
    dag_id="dag_test_extract_projeto_investimento_group_task",
    start_date=datetime.now(),
    schedule=None,
    catchup=False
)
def dag_test_extract_projeto_investimento_group_task(days, page_size, errors_limit, errors_consecutives_limit, executions_limit):
    url_base = os.getenv('URL_BASE')

    extract_projct_invest = extract_projeto_investimento(url_base, '/obrasgov/api/projeto-investimento', days, page_size, errors_limit, errors_consecutives_limit, executions_limit)
    extract_projct_invest

@dag(
    dag_id="dag_test_crud_projeto_investimento_group_task",
    start_date=datetime.now(),
    schedule=None,
    catchup=False
)
def dag_test_crud_projeto_investimento_group_task(bulk_size):

    crud_project = crud_projeto_investimento(bulk_size)
    crud_project

dag_test_extract_projeto_investimento_group_task = dag_test_extract_projeto_investimento_group_task(days, page_size, errors_limit, errors_consecutives_limit, executions_limit)
dag_test_crud_projeto_investimento_group_task = dag_test_crud_projeto_investimento_group_task(bulk_size)