import time
import json
from pathlib import Path
import shutil
import requests
import json
from datetime import datetime
from airflow.decorators import dag, task
from airflow.models.param import Param
import os.path 

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

@task()
def extract_data_api(url_base, endpoint, year, dest_path, page_size = 100, errors_limit = -1, errors_consecutives_limit = 5, executions_limit = 200):
    success = False
    page = 0
    errors_consecutives = 0
    errors = 0
    executions = 0
    method = "GET"
    dest_path_file = dest_path + '/' + str(year) + '.json'

    Path(dest_path).mkdir(parents=True, exist_ok=True)

    if os.path.isfile(dest_path_file) == True:
        os.remove(dest_path_file)

    content_all = []

    while success == False and (errors_consecutives < errors_consecutives_limit or errors_consecutives_limit == -1) and (errors < errors_limit or errors_limit == -1) and (executions < executions_limit or executions_limit == -1):
        url = url_base + endpoint + '?' + 'pagina=' + str(page) + '&' + 'tamanhoDaPagina=' + str(page_size) + '&' + 'anoFinal=' + str(year) + '&' + 'anoInicial=' + str(year) 
        response = requests.request(method, url)
        if response.status_code == 200:
            page += 1
            errors_consecutives = 0
            executions += 1
            content_all += response.json()["content"]
            #with open(dest_path_file, 'w') as f:
            #    json.dump(response.json()["content"],f)
        elif response.status_code == 404:
            success = True 
        else:
            errors_consecutives += 1
            errors += 1
            executions += 1
            if response.status_code == 429:
                time.sleep(1)
        print(f'Status Code: {response.status_code}\n'
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

@task()
def generate_dates(initial_year, final_year):
    years = []
    for year in range(int(initial_year), int(final_year) + 1):
        years.append(year)
    return years
        
@dag(
    schedule = '@daily',
    start_date = datetime.now(),
    catchup = False,
    params={
        "initial_year": Param(initial_year, type="integer"),
        "final_year": Param(final_year, type="integer"),
    },
) 
def get_api_data(url_base, endpoint, initial_year, final_year, dest_path, page_size, errors_limit, errors_consecutives_limit, executions_limit):
    years = generate_dates(initial_year, final_year)
    extract_data_api.partial(url_base=url_base, endpoint=endpoint, dest_path=dest_path, page_size=page_size,
                             errors_limit = errors_limit, errors_consecutives_limit = errors_consecutives_limit, executions_limit = executions_limit).expand(year=years)

dag = get_api_data(url_base, endpoint, initial_year, final_year, dest_path, page_size, errors_limit, errors_consecutives_limit, executions_limit)