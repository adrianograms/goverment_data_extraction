import time
import json
from pathlib import Path
import shutil
import requests
import json
from datetime import datetime

tamanho_pagina = 100
url_base = "https://api.obrasgov.gestao.gov.br"
endpoint = "/obrasgov/api/execucao-financeira"
current_year = datetime.now().year
ano_final = str(current_year)
ano_inicial = str(current_year)
dest_path = '../../database/dest/bronze/execucao-financeira'

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

def extract_data_api():
    