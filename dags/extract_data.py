import time
import json
from pathlib import Path
import requests
import json
from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
from airflow.models.param import Param
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, substring, explode, to_date, col
from pyspark.sql.types import ArrayType
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
ufs = ['PR']

def generate_sql_deletion_ufs(tables, ufs):
    sql_full = ''
    for table in tables:
        sql_full += f'''delete from {table} stg
                        using stg_projeto_investimento stg_project
                        where stg_project."idunico" = stg."idunico" and stg_project.uf in ({",".join("'" + uf + "'" for uf in ufs)});\n'''
    return sql_full

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
    return days_date

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

@task(max_active_tis_per_dag=2)
def extract_json_projeto_investimento_date(dates):
    user_dw = os.getenv('USER_DW')
    password_dw = os.getenv('PASSWORD_DW')
    host_dw = os.getenv('HOST_DW')
    database_dw = os.getenv('DATABASE_DW')
    port_dw = os.getenv('PORT_DW')
    driver = os.getenv('DRIVER_JDBC_POSTGRES')
    path_jdbc = os.getenv('PATH_JDBC_POSTGRES')
    origin = os.getenv('PATH_DEST_PROJETO_INVESTIMENTO_DATE')
    url = f'jdbc:postgresql://{host_dw}:{port_dw}/{database_dw}'
    mode = 'append'
    properties = {"user": user_dw, "password": password_dw, "driver": driver}
    origins = []

    spark = SparkSession\
        .builder\
        .appName("Extraction_Data")\
        .config("spark.driver.extraClassPath", path_jdbc)\
        .getOrCreate()
    
    for date in dates:
        origin_file = origin + f'/{date}.json'
        if os.path.isfile(origin_file) == True:
            origins.append(origin_file)

    if len(origins) == 0:
        return
    
    df = spark.read.json(origin_file)
    df = df.withColumn('nomeArquivo',substring(input_file_name(),-9,4))

    if df.count() == 0:
        return

    if 'fontesDeRecurso' in df.columns:
        df_fonte_recursos = df.withColumn("fontesDeRecurso1", explode("fontesDeRecurso"))
        if df_fonte_recursos.count() > 0:
            df_fonte_recursos = df_fonte_recursos.select("idUnico","dataCadastro","fontesDeRecurso1.*")
            df_fonte_recursos = df_fonte_recursos.withColumn("dataCadastro", to_date(col("dataCadastro"), "yyyy-MM-dd"))
            df_fonte_recursos.write.jdbc(url=url, table='stg_projeto_investimento_fontes_de_recurso', mode=mode, properties=properties)
        df_fonte_recursos.unpersist()


    if 'geometrias' in df.columns:
        df_geometria = df.withColumn("geometrias1", explode("geometrias"))
        if df_geometria.count() > 0:
            df_geometria = df_geometria.select("idUnico","dataCadastro","geometrias1.*")
            df_geometria = df_geometria.withColumn("dataCadastro", to_date(col("dataCadastro"), "yyyy-MM-dd"))
            df_geometria = df_geometria.withColumn("dataCriacao", to_date(col("dataCriacao"), "yyyy-MM-dd"))
            df_geometria = df_geometria.withColumn("dataMetadado", to_date(col("dataMetadado"), "yyyy-MM-dd"))
            df_geometria.write.jdbc(url=url, table='stg_projeto_investimento_geometria', mode=mode, properties=properties)
        df_geometria.unpersist()

    if 'subTipos' in df.columns:
        df_sub_tipos = df.withColumn("subTipos1", explode("subTipos"))
        if df_sub_tipos.count() > 0:
            df_sub_tipos = df_sub_tipos.select("idUnico","dataCadastro","subTipos1.*")
            df_sub_tipos = df_sub_tipos.withColumn("dataCadastro", to_date(col("dataCadastro"), "yyyy-MM-dd"))
            df_sub_tipos.write.jdbc(url=url, table='stg_projeto_investimento_sub_tipos', mode=mode, properties=properties)
        df_sub_tipos.unpersist()


    if 'tipos' in df.columns:
        df_tipos = df.withColumn("tipos1", explode("tipos"))
        if df_tipos.count() > 0:
            df_tipos = df_tipos.select("idUnico","dataCadastro","tipos1.*")
            df_tipos = df_tipos.withColumn("dataCadastro", to_date(col("dataCadastro"), "yyyy-MM-dd"))
            df_tipos.write.jdbc(url=url, table='stg_projeto_investimento_tipos', mode=mode, properties=properties)
        df_tipos.unpersist()

    if 'repassadores' in df.columns:
        df_repassadores = df.withColumn("repassadores1", explode("repassadores"))
        if df_repassadores.count() > 0:
            df_repassadores = df_repassadores.select("idUnico","dataCadastro","repassadores1.*")
            df_repassadores = df_repassadores.withColumn("dataCadastro", to_date(col("dataCadastro"), "yyyy-MM-dd"))
            df_repassadores.write.jdbc(url=url, table='stg_projeto_investimento_repassadores', mode=mode, properties=properties)
        df_repassadores.unpersist()

    if 'executores' in df.columns:
        df_executores = df.withColumn("executores1", explode("executores"))
        if df_executores.count() > 0:
            df_executores = df_executores.select("idUnico","dataCadastro","executores1.*")
            df_executores = df_executores.withColumn("dataCadastro", to_date(col("dataCadastro"), "yyyy-MM-dd"))
            df_executores.write.jdbc(url=url, table='stg_projeto_investimento_executores', mode=mode, properties=properties)
        df_executores.unpersist()

    if 'tomadores' in df.columns:
        df_tomadores = df.withColumn("tomadores1", explode("tomadores"))
        if df_tomadores.count() > 0:
            df_tomadores = df_tomadores.select("idUnico","dataCadastro","tomadores1.*")
            df_tomadores = df_tomadores.withColumn("dataCadastro", to_date(col("dataCadastro"), "yyyy-MM-dd"))
            df_tomadores.write.jdbc(url=url, table='stg_projeto_investimento_tomadores', mode=mode, properties=properties)
        df_tomadores.unpersist()

    if 'eixos' in df.columns:
        df_eixos = df.withColumn("eixos1", explode("eixos"))
        if df_eixos.count() > 0:
            df_eixos = df_eixos.select("idUnico","dataCadastro","eixos1.*")
            df_eixos = df_eixos.withColumn("dataCadastro", to_date(col("dataCadastro"), "yyyy-MM-dd"))
            df_eixos.write.jdbc(url=url, table='stg_projeto_investimento_eixos', mode=mode, properties=properties)
        df_eixos.unpersist()

    drop_columns = []
    for column in df.schema:
        if type(column.dataType) == ArrayType:
            drop_columns.append(column.name)

    df_projeto_investimento = df.drop(*drop_columns)
    df_projeto_investimento = df_projeto_investimento.withColumn("dataCadastro", to_date(col("dataCadastro"), "yyyy-MM-dd"))
    df_projeto_investimento = df_projeto_investimento.withColumn("dataInicialPrevista", to_date(col("dataInicialPrevista"), "yyyy-MM-dd"))
    df_projeto_investimento = df_projeto_investimento.withColumn("dataFinalPrevista", to_date(col("dataFinalPrevista"), "yyyy-MM-dd"))
    df_projeto_investimento = df_projeto_investimento.withColumn("dataInicialEfetiva", to_date(col("dataInicialEfetiva"), "yyyy-MM-dd"))
    df_projeto_investimento = df_projeto_investimento.withColumn("dataFinalEfetiva", to_date(col("dataFinalEfetiva"), "yyyy-MM-dd"))
    df_projeto_investimento = df_projeto_investimento.withColumn("dataCadastro", to_date(col("dataCadastro"), "yyyy-MM-dd"))
    df_projeto_investimento = df_projeto_investimento.withColumn("dataSituacao", to_date(col("dataSituacao"), "yyyy-MM-dd"))
    df_projeto_investimento.write.jdbc(url=url, table='stg_projeto_investimento', mode=mode, properties=properties)

    df_projeto_investimento.unpersist()
    df.unpersist()


@task()
def create_array(elements):
    arr = []
    for elemnt in elements:
        arr.append(elemnt)
    return arr


@task(max_active_tis_per_dag=1)
def delete_stg_projeto_investimento(date, days):
    user_dw = os.getenv('USER_DW')
    password_dw = os.getenv('PASSWORD_DW')
    host_dw = os.getenv('HOST_DW')
    database_dw = os.getenv('DATABASE_DW')
    port_dw = os.getenv('PORT_DW')
    driver = os.getenv('DRIVER_JDBC_POSTGRES')
    path_jdbc = os.getenv('PATH_JDBC_POSTGRES')
    url = f'jdbc:postgresql://{host_dw}:{port_dw}/{database_dw}'
    date_before = date - timedelta(days=days)

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

@task(max_active_tis_per_dag=1)
def delete_stg_projeto_investimento_uf(ufs):
    user_dw = os.getenv('USER_DW')
    password_dw = os.getenv('PASSWORD_DW')
    host_dw = os.getenv('HOST_DW')
    database_dw = os.getenv('DATABASE_DW')
    port_dw = os.getenv('PORT_DW')
    driver = os.getenv('DRIVER_JDBC_POSTGRES')
    path_jdbc = os.getenv('PATH_JDBC_POSTGRES')
    url = f'jdbc:postgresql://{host_dw}:{port_dw}/{database_dw}'
    tables = ['stg_projeto_investimento_eixos', 'stg_projeto_investimento_tomadores', 'stg_projeto_investimento_executores',
            'stg_projeto_investimento_repassadores', 'stg_projeto_investimento_tipos', 'stg_projeto_investimento_sub_tipos',
            'stg_projeto_investimento_geometria', 'stg_projeto_investimento_fontes_de_recurso']

    sql = generate_sql_deletion_ufs(tables, ufs)
    sql += f'delete from stg_projeto_investimento where uf in ({",".join("'" + uf + "'"  for uf in ufs)});\n'

    conn = jaydebeapi.connect(driver, url, [user_dw, password_dw], path_jdbc)
    curs = conn.cursor()
    curs.execute(sql)


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
    dates = generate_dates(today, days)

    extract_api = extract_data_api_projecto_investimento_date.partial(url_base=url_base, endpoint=endpoint, page_size=page_size,
                             errors_limit = errors_limit, errors_consecutives_limit = errors_consecutives_limit, executions_limit = executions_limit).expand(date=dates)
    del_stgs = delete_stg_projeto_investimento(today, days)
    extract_json = extract_json_projeto_investimento_date(dates)

    extract_api >> extract_json
    del_stgs >> extract_json

@task_group()
def extract_projeto_investimento_uf(url_base, endpoint, ufs, page_size, errors_limit, errors_consecutives_limit, executions_limit):

    ufs_array = create_array(ufs)
    extract_api = extract_data_api_project.partial(url_base=url_base, endpoint=endpoint, page_size=page_size,
                             errors_limit = errors_limit, errors_consecutives_limit = errors_consecutives_limit, executions_limit = executions_limit).expand(uf=ufs_array)
    del_stgs = delete_stg_projeto_investimento_uf(ufs)
    #extract_json = extract_json_projeto_investimento_date(dates)

    #extract_api >> extract_json
    #del_stgs >> extract_json

    

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
    
    extract_exec_fin = extract_execucao_financeira(url_base, '/obrasgov/api/execucao-financeira', initial_year, final_year, page_size, errors_limit, errors_consecutives_limit, executions_limit)
    extract_projct_invest = extract_projeto_investimento(url_base, '/obrasgov/api/projeto-investimento', days, page_size, errors_limit, errors_consecutives_limit, executions_limit)

@dag(
    schedule = '@daily',
    start_date = datetime.now(),
    catchup = False,
) 
def dag_get_projeto_investimento_uf(ufs, page_size, errors_limit, errors_consecutives_limit, executions_limit):
    url_base = os.getenv('URL_BASE')

    extract_projct_invest = extract_projeto_investimento_uf(url_base, '/obrasgov/api/projeto-investimento', ufs, page_size, errors_limit, errors_consecutives_limit, executions_limit)


dag = get_api_data(initial_year, final_year, days, page_size, errors_limit, errors_consecutives_limit, executions_limit)
dag_investimento_uf = dag_get_projeto_investimento_uf(ufs, page_size, errors_limit, errors_consecutives_limit, executions_limit)
