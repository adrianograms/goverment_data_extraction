import time
import json
from pathlib import Path
import requests
import json
from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
from airflow.models.param import Param
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, substring, explode, to_date, col, regexp_replace
from pyspark.sql.types import ArrayType
import os.path 
import jaydebeapi
import os
from dotenv import load_dotenv
from functools import reduce
import  psycopg2 
from math import ceil
from tasks.operations_task import * 

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
ufs = ['PR']    

@dag(
    schedule = None,
    start_date = datetime.now(),
    catchup = False,
    params={
        "initial_year": Param(initial_year, type="integer"),
        "final_year": Param(final_year, type="integer"),
    },
) 
def get_api_data(initial_year, final_year, days, page_size, errors_limit, errors_consecutives_limit, executions_limit):
    url_base = os.getenv('URL_BASE')
    bulk_size = int(os.getenv('BULK_SIZE'))
    
    extract_exec_fin = extract_execucao_financeira(url_base, '/obrasgov/api/execucao-financeira', initial_year, final_year, page_size, errors_limit, errors_consecutives_limit, executions_limit)
    extract_projct_invest = extract_projeto_investimento(url_base, '/obrasgov/api/projeto-investimento', days, page_size, errors_limit, errors_consecutives_limit, executions_limit)
    crud_project = crud_projeto_investimento(bulk_size)

    extract_exec_fin >> crud_project
    extract_projct_invest >> crud_project

@dag(
    schedule = None,
    start_date = datetime.now(),
    catchup = False,
) 
def dag_get_projeto_investimento_uf(ufs, page_size, errors_limit, errors_consecutives_limit, executions_limit):
    url_base = os.getenv('URL_BASE')

    extract_projct_invest = extract_projeto_investimento_uf(url_base, '/obrasgov/api/projeto-investimento', ufs, page_size, errors_limit, errors_consecutives_limit, executions_limit)


dag = get_api_data(initial_year, final_year, days, page_size, errors_limit, errors_consecutives_limit, executions_limit)
dag_investimento_uf = dag_get_projeto_investimento_uf(ufs, page_size, errors_limit, errors_consecutives_limit, executions_limit)
