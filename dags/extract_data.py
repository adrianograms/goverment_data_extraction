import time
import json
from pathlib import Path
import requests
import json
from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
from airflow.models.param import Param
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, substring
import os.path 
import jaydebeapi
import os
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

def generate_url(url_base, endpoint, parameters):
    url = url_base + endpoint
    i = 0
    number_parameters = len(parameters)
    if number_parameters > 0:
        url += '?'
    for parameter in parameters.items():
        url += parameter[0] + '=' + str(parameter[1])
        i += 1
        if i != number_parameters:
            url += '&'
    return url

def extract_api(url_base, endpoint, param, method, path_dest_env, fun_api, page_size, errors_limit, errors_consecutives_limit, executions_limit):
    success = False
    page = 0
    errors_consecutives = 0
    errors = 0
    executions = 0
    dest_path = os.getenv(path_dest_env)
    dest_path_file = dest_path + '/' + str(param) + '.json'

    Path(dest_path).mkdir(parents=True, exist_ok=True)

    if os.path.isfile(dest_path_file) == True:
        os.remove(dest_path_file)

    content_all = []
    while success == False and (errors_consecutives < errors_consecutives_limit or errors_consecutives_limit == -1) and (errors < errors_limit or errors_limit == -1) and (executions < executions_limit or executions_limit == -1):
        success, content,  page, errors_consecutives, errors, executions, status_code =  fun_api(url_base, endpoint, param, method, page_size, page, errors_consecutives, errors, executions)

        if content is not None:
            content_all += content

        print(f'Status Code: {status_code}\n'
            f'Executions: {executions}\n'
            f'Pages: {page}\n' 
            f'N° Registers: {len(content_all)}\n'
            f'Errors: {errors}\n'
            f'Errors Consecutives: {errors_consecutives}\n')
        time.sleep(1)

    if success == True:
        print('Execution Finished with success!')
    else:
        print('Execution Finished with error!')
        if errors_consecutives < errors_consecutives_limit:
            raise Exception("Number of consecutives errors exceeded")
        elif errors < errors_limit:
            raise Exception("Number of total errors exceeded")
        elif executions < executions_limit:
            raise Exception("Number of executions exceeded")

    with open(dest_path_file, 'w', encoding='utf-8') as f:
        json.dump(content_all, f)
    f.close()

def extract_api_projeto_investimento_uf(url_base, endpoint, param, method, page_size, page, errors_consecutives, errors, executions):
    params = {"pagina": str(page), "tamanhoDaPagina": str(page_size), "uf": str(param)}
    url = generate_url(url_base, endpoint, params)
    response = requests.request(method, url)
    success = False
    if response.status_code == 200:
        if(len(response.json()["content"]) == 0):
            success = True
            return success, None,  page, errors_consecutives, errors, executions, response.status_code
        else:
            page += 1
            errors_consecutives = 0
            executions += 1
            content = response.json()["content"]
            return success, content,  page, errors_consecutives, errors, executions, response.status_code
    else:
        errors_consecutives += 1
        errors += 1
        executions += 1
        if response.status_code == 429:
            time.sleep(1)
        return success, None,  page, errors_consecutives, errors, executions, response.status_code
    
def extract_api_projeto_investimento_date(url_base, endpoint, param, method, page_size, page, errors_consecutives, errors, executions):
    params = {"pagina": str(page), "tamanhoDaPagina": str(page_size), "dataCadastro": str(param)}
    url = generate_url(url_base, endpoint, params)
    response = requests.request(method, url)
    success = False
    if response.status_code == 200:
        if(len(response.json()["content"]) == 0):
            success = True
            return success, None,  page, errors_consecutives, errors, executions, response.status_code
        else:
            page += 1
            errors_consecutives = 0
            executions += 1
            content = response.json()["content"]
            return success, content,  page, errors_consecutives, errors, executions, response.status_code
    else:
        errors_consecutives += 1
        errors += 1
        executions += 1
        if response.status_code == 429:
            time.sleep(1)
        return success, None,  page, errors_consecutives, errors, executions, response.status_code

def extract_api_execucao_financeira_year(url_base, endpoint, year, method, page_size, page, errors_consecutives, errors, executions):
    params = {"pagina": str(page), "tamanhoDaPagina": str(page_size), "anoFinal": str(year), "anoInicial": str(year)}
    url = generate_url(url_base, endpoint, params)
    success = False
    response = requests.request(method, url)
    if response.status_code == 200:
        page += 1
        errors_consecutives = 0
        executions += 1
        content = response.json()["content"]
        return success, content,  page, errors_consecutives, errors, executions, response.status_code
    elif response.status_code == 404:
        success = True
        return success, None,  page, errors_consecutives, errors, executions, response.status_code 
    else:
        errors_consecutives += 1
        errors += 1
        executions += 1
        if response.status_code == 429:
            time.sleep(1)
        return success, None,  page, errors_consecutives, errors, executions, response.status_code

@task()
def extract_data_api(url_base, endpoint, year, page_size = 100, errors_limit = -1, errors_consecutives_limit = 5, executions_limit = 200):
    method = "GET"
    path_dest_env = "PATH_DEST"
    extract_api(url_base, endpoint, year, method, path_dest_env, extract_api_execucao_financeira_year, page_size, errors_limit, errors_consecutives_limit, executions_limit)

@task()
def extract_data_api_project(url_base, endpoint, uf, page_size = 100, errors_limit = -1, errors_consecutives_limit = 5, executions_limit = 200):
    method = "GET"
    path_dest_env = "PATH_DEST_PROJETO_INVESTIMENTO_UF"
    extract_api(url_base, endpoint, uf, method, path_dest_env, extract_api_projeto_investimento_uf, page_size, errors_limit, errors_consecutives_limit, executions_limit)

@task(max_active_tis_per_dag=10)
def extract_data_api_projecto_investimento_date(url_base, endpoint, date, page_size = 100, errors_limit = -1, errors_consecutives_limit = 5, executions_limit = 200):
    method = "GET"
    path_dest_env = "PATH_DEST_PROJETO_INVESTIMENTO_DATE"
    extract_api(url_base, endpoint, date, method, path_dest_env, extract_api_projeto_investimento_date, page_size, errors_limit, errors_consecutives_limit, executions_limit)

@task()
def generate_years(initial_year, final_year):
    years = []
    for year in range(int(initial_year), int(final_year) + 1):
        years.append(year)
    return years

@task()
def generate_dates(date, days):
    days_date = []
    date_before = date - timedelta(days=days)
    date_generated = [date_before + timedelta(days=x) for x in range(0, (date-date_before).days)]

    for d in date_generated:
        days_date.append(str(d.strftime('%Y-%m-%d')))
    return days_date, date_before

@task(max_active_tis_per_dag=1)
def extract_data_json(year):
    user_dw = os.getenv('USER_DW')
    password_dw = os.getenv('PASSWORD_DW')
    host_dw = os.getenv('HOST_DW')
    database_dw = os.getenv('DATABASE_DW')
    port_dw = os.getenv('PORT_DW')
    driver = os.getenv('DRIVER_JDBC_POSTGRES')
    path_jdbc = os.getenv('PATH_JDBC_POSTGRES')
    origin = os.getenv('PATH_DEST')
    url = f'jdbc:postgresql://{host_dw}:{port_dw}/{database_dw}'

    spark = SparkSession\
        .builder\
        .appName("Extraction_Data")\
        .config("spark.driver.extraClassPath", path_jdbc)\
        .getOrCreate()
    origin_file = origin + '/' + str(year)+ '.json'
    if os.path.isfile(origin_file) == True:
        df = spark.read.json(origin_file)
        df = df.withColumn('nomeArquivo',substring(input_file_name(),-9,4))
    else:
        raise Exception("File dosen't exist!")

    conn = jaydebeapi.connect(driver, url, [user_dw, password_dw], path_jdbc)
    curs = conn.cursor()
    curs.execute(f'''delete from stg_execucao_financeira where "nomeArquivo" = '{year}' ''')

    mode = 'append'
    properties = {"user": user_dw, "password": password_dw, "driver": driver}
    df.write.jdbc(url=url, table='stg_execucao_financeira', mode=mode, properties=properties)

@task(max_active_tis_per_dag=1)
def delete_stg_projeto_investimento(date, date_before):
    user_dw = os.getenv('USER_DW')
    password_dw = os.getenv('PASSWORD_DW')
    host_dw = os.getenv('HOST_DW')
    database_dw = os.getenv('DATABASE_DW')
    port_dw = os.getenv('PORT_DW')
    driver = os.getenv('DRIVER_JDBC_POSTGRES')
    path_jdbc = os.getenv('PATH_JDBC_POSTGRES')
    url = f'jdbc:postgresql://{host_dw}:{port_dw}/{database_dw}'


    conn = jaydebeapi.connect(driver, url, [user_dw, password_dw], path_jdbc)
    curs = conn.cursor()
    curs.execute(f'''delete from stg_projeto_investimento_eixos     where "datacadastro" between '{date_before}' and '{date}';
                 delete from stg_projeto_investimento_tomadores     where "datacadastro" between '{date_before}' and '{date}';
                 delete from stg_projeto_investimento_executores    where "datacadastro" between '{date_before}' and '{date}';
                 delete from stg_projeto_investimento_repassadores  where "datacadastro" between '{date_before}' and '{date}';
                 delete from stg_projeto_investimento_tipos         where "datacadastro" between '{date_before}' and '{date}';
                 delete from stg_projeto_investimento_sub_tipos     where "datacadastro" between '{date_before}' and '{date}';
                 delete from stg_projeto_investimento_geometria     where "datacadastro" between '{date_before}' and '{date}';
                 delete from stg_projeto_investimento_fontes_de_recurso where "datacadastro" between '{date_before}' and '{date}';
                 delete from stg_projeto_investimento               where "datacadastro" between '{date_before}' and '{date}';''')


@task_group()
def extract_execucao_financeira(url_base, endpoint, initial_year, final_year, page_size, errors_limit, errors_consecutives_limit, executions_limit):
    years = generate_years(initial_year, final_year)
    extract_api = extract_data_api.partial(url_base=url_base, endpoint=endpoint, page_size=page_size,
                             errors_limit = errors_limit, errors_consecutives_limit = errors_consecutives_limit, executions_limit = executions_limit).expand(year=years)
    extract_json = extract_data_json.expand(year=years)
    extract_api >> extract_json

@task_group()
def extract_projeto_investimento(url_base, endpoint, days, page_size, errors_limit, errors_consecutives_limit, executions_limit):
    today = datetime.now()
    dates, date_before = generate_dates(today, days)
    extract_api = extract_data_api_projecto_investimento_date.partial(url_base=url_base, endpoint=endpoint, page_size=page_size,
                             errors_limit = errors_limit, errors_consecutives_limit = errors_consecutives_limit, executions_limit = executions_limit).expand(date=dates)
    del_stgs = delete_stg_projeto_investimento(today, date_before)

    

@dag(
    schedule = '@daily',
    start_date = datetime.now(),
    catchup = False,
    params={
        "initial_year": Param(initial_year, type="integer"),
        "final_year": Param(final_year, type="integer"),
    },
) 
def get_api_data(initial_year, final_year, days, page_size, errors_limit, errors_consecutives_limit, executions_limit):
    url_base = os.getenv('URL_BASE')
    ufs = ['AC']

    extract_exec_fin = extract_execucao_financeira(url_base, '/obrasgov/api/execucao-financeira', initial_year, final_year, page_size, errors_limit, errors_consecutives_limit, executions_limit)

    extract_projct_invest = extract_projeto_investimento(url_base, '/obrasgov/api/projeto-investimento', days, page_size, errors_limit, errors_consecutives_limit, executions_limit)

    #extract_api_project = extract_data_api_project.partial(url_base=url_base, endpoint='/obrasgov/api/projeto-investimento', page_size=page_size,
    #                         errors_limit = errors_limit, errors_consecutives_limit = errors_consecutives_limit, executions_limit = executions_limit).expand(uf=ufs)

    #extract_api = extract_data_api.partial(url_base=url_base, endpoint=endpoint, page_size=page_size,
    #                         errors_limit = errors_limit, errors_consecutives_limit = errors_consecutives_limit, executions_limit = executions_limit).expand(year=years)
    #extract_json = extract_data_json.expand(year=years)
    #extract_api >> extract_json

dag = get_api_data(initial_year, final_year, days, page_size, errors_limit, errors_consecutives_limit, executions_limit)
