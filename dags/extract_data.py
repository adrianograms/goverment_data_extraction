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

tamanho_pagina = 100
url_base = "https://api.obrasgov.gestao.gov.br"
endpoint = "/obrasgov/api/execucao-financeira"
current_year = datetime.now().year
ano_inicial = int(current_year)
ano_final = int(current_year)
dest_path = '/home/adriano/Documentos/airflow/database/dest/bronze/execucao-financeira'

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
def extract_data_api(url_base, endpoint, ano, dest_path, tamanho_pagina = 100):
    success = False
    pagina = 0
    errors_consecutives = 0
    errors_consecutives_limit = 5
    errors = 0
    errors_limit = 50
    executions = 0
    executions_limit = 200
    method = "GET"
    dest_path_file = dest_path + '/' + str(ano) + '.json'

    Path(dest_path).mkdir(parents=True, exist_ok=True)

    if os.path.isfile(dest_path_file) == True:
        shutil.rmtree(dest_path_file)

    content_all = []

    while success == False and errors_consecutives < errors_consecutives_limit and errors < errors_limit and executions < executions_limit:
        url = url_base + endpoint + '?' + 'pagina=' + str(pagina) + '&' + 'tamanhoDaPagina=' + str(tamanho_pagina) + '&' + 'anoFinal=' + str(ano) + '&' + 'anoInicial=' + str(ano) 
        response = requests.request(method, url)
        if response.status_code == 200:
            pagina += 1
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
            f'Pagina: {pagina}\n' 
            f'N° De registros: {len(content_all)}\n'
            f'Erros: {errors}\n'
            f'Erros Consecutivos: {errors_consecutives}\n')
        time.sleep(1)

    if success == True:
        print('Execução Finalizada com sucesso!')
    else:
        print('Execução Finalizada com falha!')
        if errors_consecutives < errors_consecutives_limit:
            raise Exception("Número de erros consecutivos excedido")
        elif errors < errors_limit:
            raise Exception("Número de erros total excedido")
        elif executions < executions_limit:
            raise Exception("Número de execuções excedido")

    with open(dest_path_file, 'w', encoding='utf-8') as f:
        json.dump(content_all, f)
    f.close()
        
@dag(
    schedule = '@daily',
    start_date = datetime.now(),
    catchup = False,
    params ={
        "ano_inicial": Param(
            ano_inicial,
            type="integer",
        ),
        "ano_final": Param(
            ano_final,
            type="integer",
        )
    }
)
def get_api_data(url_base, endpoint, ano_inicial, ano_final, dest_path, tamanho_pagina):
    dif_anos = (ano_final - ano_inicial) + 1
    for i in range(dif_anos):
        ano = ano_inicial + i
        extract_data_api.override(task_id='execucao-financeira')(url_base, endpoint, ano, dest_path, tamanho_pagina)

dag = get_api_data(url_base, endpoint, ano_inicial, ano_final, dest_path, tamanho_pagina)