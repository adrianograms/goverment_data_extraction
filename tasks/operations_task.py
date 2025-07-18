from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
from airflow.models.param import Param
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, substring, explode, to_date, col, regexp_replace
from pyspark.sql.types import ArrayType
import jaydebeapi
import os
from dotenv import load_dotenv
from scripts.functions_util import *

@task()
def delete_duplicates_stg():
    user_dw = os.getenv('USER_DW')
    password_dw = os.getenv('PASSWORD_DW')
    host_dw = os.getenv('HOST_DW')
    database_dw = os.getenv('DATABASE_DW')
    port_dw = os.getenv('PORT_DW')
    driver = os.getenv('DRIVER_JDBC_POSTGRES')
    path_jdbc = os.getenv('PATH_JDBC_POSTGRES')

    connection_properties = {'db_name':database_dw, 'user':user_dw, 'password':password_dw, 'host':host_dw, 'port':port_dw, 'driver': driver}
    tables_key_columns = [('stg_projeto_investimento_eixos', ['idunico','id']),
                          ('stg_projeto_investimento_executores', ['idunico', 'codigo']),
                          ('stg_projeto_investimento_fontes_de_recurso', ['idunico', 'origem', 'valorinvestimentoprevisto']),
                          ('stg_projeto_investimento_repassadores', ['idunico', 'codigo']),
                          ('stg_projeto_investimento_sub_tipos', ['idunico', 'id']),
                          ('stg_projeto_investimento_tipos', ['idunico', 'id']),
                          ('stg_projeto_investimento_tomadores', ['idunico', 'codigo']),
                          ('stg_projeto_investimento', ['idunico'])]
    for table_name, key_columns in tables_key_columns:
        print(f'Deleting duplicates on table {table_name}...')
        delete_duplicates(table_name, key_columns, connection_properties)

    tables_distinct_delete = ['stg_execucao_financeira']
    for table_name in tables_distinct_delete:
        print(f'Deleting duplicates on table {table_name} using distinct...')
        delete_duplicates_distinct(table_name,connection_properties)

@task()
def crud_dimensions(bulk_size):
    user_dw = os.getenv('USER_DW')
    password_dw = os.getenv('PASSWORD_DW')
    host_dw = os.getenv('HOST_DW')
    database_dw = os.getenv('DATABASE_DW')
    port_dw = os.getenv('PORT_DW')
    driver = os.getenv('DRIVER_JDBC_POSTGRES')
    path_jdbc = os.getenv('PATH_JDBC_POSTGRES')

    connection_properties = {'db_name':database_dw, 'user':user_dw, 'password':password_dw, 'host':host_dw, 'port':port_dw, 'driver': driver}

    spark = SparkSession\
        .builder\
        .appName("Extraction_Data")\
        .config("spark.driver.extraClassPath", path_jdbc)\
        .getOrCreate()

    sql_new_eixos = 'select distinct id as nk_eixo, descricao from stg_projeto_investimento_eixos'
    sql_old_eixos = 'select nk_eixo, descricao from dim_eixo de'
    table_name_eixos = 'public.dim_eixo'
    key_columns_eixos = ['nk_eixo']
    crud_database_table(spark, sql_old_eixos, sql_new_eixos, table_name_eixos, key_columns_eixos, connection_properties, bulk_size)

    sql_new_executores = 'select distinct codigo as nk_executor, nome as descricao from stg_projeto_investimento_executores'
    sql_old_executores = 'select nk_executor, descricao from dim_executor'
    table_name_executores = 'public.dim_executor'
    key_columns_executores = ['nk_executor']
    crud_database_table(spark, sql_old_executores, sql_new_executores, table_name_executores, key_columns_executores, connection_properties, bulk_size)

    sql_new_fonte_recurso = 'select distinct UPPER(origem) as nk_fonte_recurso, origem as descricao from stg_projeto_investimento_fontes_de_recurso'
    sql_old_fonte_recurso = 'select nk_fonte_recurso, descricao from dim_fonte_recurso'
    table_name_fonte_recurso = 'public.dim_fonte_recurso'
    key_columns_fonte_recurso = ['nk_fonte_recurso']
    crud_database_table(spark, sql_old_fonte_recurso, sql_new_fonte_recurso, table_name_fonte_recurso, key_columns_fonte_recurso, connection_properties, bulk_size)

    sql_new_repassador = 'select distinct codigo as nk_repassador, nome as descricao from stg_projeto_investimento_repassadores'
    sql_old_repassador = 'select nk_repassador, descricao from dim_repassador'
    table_name_repassador = 'public.dim_repassador'
    key_columns_repassador = ['nk_repassador']
    crud_database_table(spark, sql_old_repassador, sql_new_repassador, table_name_repassador, key_columns_repassador, connection_properties, bulk_size)

    sql_new_sub_tipo = 'select distinct id as nk_sub_tipo, descricao from stg_projeto_investimento_sub_tipos'
    sql_old_sub_tipo = 'select nk_sub_tipo, descricao from dim_sub_tipo'
    table_name_sub_tipo = 'public.dim_sub_tipo'
    key_columns_sub_tipo = ['nk_sub_tipo']
    crud_database_table(spark, sql_old_sub_tipo, sql_new_sub_tipo, table_name_sub_tipo, key_columns_sub_tipo, connection_properties, bulk_size)

    sql_new_tipo = 'select distinct id as nk_tipo, descricao from stg_projeto_investimento_tipos'
    sql_old_tipo = 'select nk_tipo, descricao from dim_tipo'
    table_name_tipo = 'public.dim_tipo'
    key_columns_tipo = ['nk_tipo']
    crud_database_table(spark, sql_old_tipo, sql_new_tipo, table_name_tipo, key_columns_tipo, connection_properties, bulk_size)

    sql_new_tomador = 'select distinct codigo as nk_tomador, nome as descricao from stg_projeto_investimento_tomadores'
    sql_old_tomador = 'select nk_tomador, descricao from dim_tomador'
    table_name_tomador = 'public.dim_tomador'
    key_columns_tomador = ['nk_tomador']
    crud_database_table(spark, sql_old_tomador, sql_new_tomador, table_name_tomador, key_columns_tomador, connection_properties, bulk_size)

    sql_new_uf = 'select distinct upper(uf) as nk_uf, uf as descricao from stg_projeto_investimento '
    sql_old_uf = 'select nk_uf, descricao from dim_uf'
    table_name_uf = 'public.dim_uf'
    key_columns_uf = ['nk_uf']
    crud_database_table(spark, sql_old_uf, sql_new_uf, table_name_uf, key_columns_uf, connection_properties, bulk_size)

    sql_new_situacao = 'select distinct upper(situacao) as nk_situacao, situacao as descricao from stg_projeto_investimento'
    sql_old_situacao = 'select nk_situacao, descricao from dim_situacao ds'
    table_name_situacao = 'public.dim_situacao'
    key_columns_situacao = ['nk_situacao']
    crud_database_table(spark, sql_old_situacao, sql_new_situacao, table_name_situacao, key_columns_situacao, connection_properties, bulk_size)

    sql_new_natureza = 'select distinct upper(natureza) as nk_natureza, natureza as descricao from stg_projeto_investimento'
    sql_old_natureza = 'select nk_natureza, descricao from dim_natureza dn '
    table_name_natureza = 'public.dim_natureza'
    key_columns_natureza = ['nk_natureza']
    crud_database_table(spark, sql_old_natureza, sql_new_natureza, table_name_natureza, key_columns_natureza, connection_properties, bulk_size)

    sql_new_especie = 'select distinct upper(especie) as nk_especie, especie as descricao from stg_projeto_investimento' 
    sql_old_especie = 'select nk_especie, descricao from dim_especie de '
    table_name_especie = 'public.dim_especie'
    key_columns_especie = ['nk_especie']
    crud_database_table(spark, sql_old_especie, sql_new_especie, table_name_especie, key_columns_especie, connection_properties, bulk_size)

    sql_new_projeto_investimento = '''select distinct 
                                            idunico as nk_projeto_investimento,
                                            datacadastro as data_projeto,
                                            nome as nome_projeto,
                                            cep,
                                            endereco,
                                            descricao as descricao_projeto,
                                            funcaosocial as funcao_social,
                                            metaglobal as meta_global,
                                            datainicialprevista as data_inicial_prevista,
                                            datafinalprevista as data_final_prevista,
                                            datainicialefetiva as data_inicial_efetiva,
                                            datafinalefetiva as data_final_efetiva,
                                            especie,
                                            natureza,
                                            naturezaoutras as natureza_outras,
                                            situacao
                                        from stg_projeto_investimento spi''' 
    sql_old_projeto_investimento = '''select 
                                        nk_projeto_investimento,
                                        data_projeto,
                                        nome_projeto,
                                        cep,
                                        endereco,
                                        descricao_projeto,
                                        funcao_social,
                                        meta_global,
                                        data_inicial_prevista,
                                        data_final_prevista,
                                        data_inicial_efetiva,
                                        data_final_efetiva,
                                        especie,
                                        natureza,
                                        natureza_outras,
                                        situacao
                                    from dim_projeto_investimento'''
    table_name_projeto_investimento = 'public.dim_projeto_investimento'
    key_columns_projeto_investimento = ['nk_projeto_investimento']
    crud_database_table(spark, sql_old_projeto_investimento, sql_new_projeto_investimento, table_name_projeto_investimento, key_columns_projeto_investimento, connection_properties, bulk_size)

@task()
def crud_facts(bulk_size):
    user_dw = os.getenv('USER_DW')
    password_dw = os.getenv('PASSWORD_DW')
    host_dw = os.getenv('HOST_DW')
    database_dw = os.getenv('DATABASE_DW')
    port_dw = os.getenv('PORT_DW')
    driver = os.getenv('DRIVER_JDBC_POSTGRES')
    path_jdbc = os.getenv('PATH_JDBC_POSTGRES')

    connection_properties = {'db_name':database_dw, 'user':user_dw, 'password':password_dw, 'host':host_dw, 'port':port_dw, 'driver': driver}

    spark = SparkSession\
        .builder\
        .appName("Extraction_Data")\
        .config("spark.driver.extraClassPath", path_jdbc)\
        .getOrCreate()

    # CRUD fact_projeto_investimento
    sql_new_project_inv =     '''WITH INVESTIMENTO_PREVISTO AS (
                                    SELECT 	IDUNICO,
                                            SUM(STG.VALORINVESTIMENTOPREVISTO) AS VALOR_INVESTIMENTO_PREVISTO
                                    FROM STG_PROJETO_INVESTIMENTO_FONTES_DE_RECURSO STG 
                                    GROUP BY 1
                                ),
                                EXECUCAO_FINANCEIRA AS (
                                    SELECT 
                                            STG."idProjetoInvestimento" AS IDUNICO,
                                            SUM(STG."valorEmpenho") AS VALOR_EXECUCAO
                                    FROM STG_EXECUCAO_FINANCEIRA STG
                                    GROUP BY 1
                                )
                                SELECT 
                                        DPI.SK_PROJETO_INVESTIMENTO,
                                        STG.DATAFINALPREVISTA - STG.DATAINICIALPREVISTA AS PRAZO_PREVISTO,
                                        STG.DATAFINALEFETIVA - STG.DATAINICIALEFETIVA AS PRAZO_EFETIVO,
                                        CAST(NULLIF(STG.QDTEMPREGOSGERADOS, '') AS INTEGER) AS QTD_EMPREGOS_GERADOS,
                                        CAST(NULLIF(STG.POPULACAOBENEFICIADA, '') AS INTEGER)  AS POP_BENEFICIADA,
                                        IP.VALOR_INVESTIMENTO_PREVISTO,
                                        COALESCE(EF.VALOR_EXECUCAO,0) AS VALOR_EXECUCAO
                                FROM STG_PROJETO_INVESTIMENTO STG
                                INNER JOIN DIM_PROJETO_INVESTIMENTO 	DPI ON DPI.NK_PROJETO_INVESTIMENTO = STG.IDUNICO
                                LEFT JOIN INVESTIMENTO_PREVISTO 		IP  ON IP.IDUNICO = STG.IDUNICO
                                LEFT JOIN EXECUCAO_FINANCEIRA 			EF  ON EF.IDUNICO = STG.IDUNICO '''
    sql_old_project_inv =     '''SELECT sk_projeto_investimento, 
                                        prazo_previsto, 
                                        prazo_efetivo, 
                                        qtd_empregos_gerados, 
                                        pop_beneficiada, 
                                        valor_investimento_previsto, 
                                        valor_execucao
                                FROM public.fact_projeto_investimento'''
    table_name_project_inv = 'public.fact_projeto_investimento'
    key_columns_project_inv  = ['sk_projeto_investimento']
    crud_database_table(spark, sql_old_project_inv, sql_new_project_inv, table_name_project_inv, key_columns_project_inv, connection_properties, bulk_size)

    # CRUD fact_projeto_investimento_fonte_recurso
    sql_new_project_inv_font_rec =     '''SELECT 
                                                DPI.SK_PROJETO_INVESTIMENTO,
                                                DFR.SK_FONTE_RECURSO,
                                                FR.VALORINVESTIMENTOPREVISTO AS VALOR_INVESTIMENTO_PREVISTO
                                        FROM STG_PROJETO_INVESTIMENTO STG
                                        INNER JOIN DIM_PROJETO_INVESTIMENTO 	DPI ON DPI.NK_PROJETO_INVESTIMENTO = STG.IDUNICO
                                        INNER JOIN STG_PROJETO_INVESTIMENTO_FONTES_DE_RECURSO FR ON FR.IDUNICO = STG.IDUNICO 
                                        INNER JOIN DIM_FONTE_RECURSO            DFR ON DFR.NK_FONTE_RECURSO = UPPER(FR.ORIGEM)'''
    sql_old_project_inv_font_rec =     '''SELECT 
                                                    SK_PROJETO_INVESTIMENTO,
                                                    SK_FONTE_RECURSO,
                                                    VALOR_INVESTIMENTO_PREVISTO
                                            FROM FACT_PROJETO_INVESTIMENTO_FONTE_RECURSO FACT '''
    table_name_project_inv_font_rec = 'public.FACT_PROJETO_INVESTIMENTO_FONTE_RECURSO'
    key_columns_project_inv_font_rec  = ['sk_projeto_investimento', 'sk_fonte_recurso']
    crud_database_table(spark, sql_old_project_inv_font_rec, sql_new_project_inv_font_rec, table_name_project_inv_font_rec, key_columns_project_inv_font_rec, connection_properties, bulk_size)

    # CRUD fact_projeto_investimento_eixos
    sql_new_project_inv_eixos =     '''SELECT 	DISTINCT
                                                DPI.SK_PROJETO_INVESTIMENTO,
                                                DE.SK_EIXO
                                        FROM STG_PROJETO_INVESTIMENTO_EIXOS STG
                                        INNER JOIN DIM_EIXO DE ON DE.NK_EIXO = CAST(STG.ID AS VARCHAR)
                                        INNER JOIN DIM_PROJETO_INVESTIMENTO DPI ON DPI.NK_PROJETO_INVESTIMENTO = STG.IDUNICO'''
    sql_old_project_inv_eixos =     '''SELECT
                                                FATO.SK_PROJETO_INVESTIMENTO,
                                                FATO.SK_EIXO
                                        FROM FACT_PROJETO_INVESTIMENTO_EIXOS FATO '''
    table_name_project_inv_eixos = 'public.FACT_PROJETO_INVESTIMENTO_EIXOS'
    key_columns_project_inv_eixos  = ['sk_projeto_investimento', 'sk_eixo']
    crud_database_table(spark, sql_old_project_inv_eixos, sql_new_project_inv_eixos, table_name_project_inv_eixos, key_columns_project_inv_eixos, connection_properties, bulk_size)

    # CRUD fact_projeto_investimento_tomadores
    sql_new_project_inv_tom =     '''SELECT 	DISTINCT
                                            DPI.SK_PROJETO_INVESTIMENTO,
                                            DT.SK_TOMADOR
                                    FROM STG_PROJETO_INVESTIMENTO_TOMADORES STG
                                    INNER JOIN DIM_TOMADOR DT ON DT.NK_TOMADOR = CAST(STG.CODIGO AS VARCHAR) 
                                    INNER JOIN DIM_PROJETO_INVESTIMENTO DPI ON DPI.NK_PROJETO_INVESTIMENTO = STG.IDUNICO'''
    sql_old_project_inv_tom =     '''SELECT 
                                            SK_PROJETO_INVESTIMENTO,
                                            SK_TOMADOR
                                    FROM FACT_PROJETO_INVESTIMENTO_TOMADORES FATO '''
    table_name_project_inv_tom = 'public.FACT_PROJETO_INVESTIMENTO_TOMADORES'
    key_columns_project_inv_tom  = ['sk_projeto_investimento', 'sk_tomador']
    crud_database_table(spark, sql_old_project_inv_tom, sql_new_project_inv_tom, table_name_project_inv_tom, key_columns_project_inv_tom, connection_properties, bulk_size)

    # CRUD fact_projeto_investimento_executores
    sql_new_project_inv_exc =     '''SELECT 	DISTINCT
                                            DPI.SK_PROJETO_INVESTIMENTO,
                                            DE.SK_EXECUTOR 
                                    FROM STG_PROJETO_INVESTIMENTO_EXECUTORES STG
                                    INNER JOIN DIM_EXECUTOR DE ON DE.NK_EXECUTOR = CAST(STG.CODIGO AS VARCHAR) 
                                    INNER JOIN DIM_PROJETO_INVESTIMENTO DPI ON DPI.NK_PROJETO_INVESTIMENTO = STG.IDUNICO'''
    sql_old_project_inv_exc =     '''SELECT
                                            SK_PROJETO_INVESTIMENTO,
                                            SK_EXECUTOR 
                                    FROM FACT_PROJETO_INVESTIMENTO_EXECUTORES FATO '''
    table_name_project_inv_exc = 'public.FACT_PROJETO_INVESTIMENTO_EXECUTORES'
    key_columns_project_inv_exc  = ['sk_projeto_investimento', 'sk_executor']
    crud_database_table(spark, sql_old_project_inv_exc, sql_new_project_inv_exc, table_name_project_inv_exc, key_columns_project_inv_exc, connection_properties, bulk_size)

    # CRUD fact_projeto_investimento_repassadores
    sql_new_project_inv_rep =     '''SELECT 	DISTINCT
                                            DPI.SK_PROJETO_INVESTIMENTO,
                                            DR.SK_REPASSADOR 
                                    FROM STG_PROJETO_INVESTIMENTO_REPASSADORES STG
                                    INNER JOIN DIM_REPASSADOR DR ON DR.NK_REPASSADOR = CAST(STG.CODIGO AS VARCHAR) 
                                    INNER JOIN DIM_PROJETO_INVESTIMENTO DPI ON DPI.NK_PROJETO_INVESTIMENTO = STG.IDUNICO'''
    sql_old_project_inv_rep =     '''SELECT 
                                            SK_PROJETO_INVESTIMENTO,
                                            SK_REPASSADOR 
                                    FROM FACT_PROJETO_INVESTIMENTO_REPASSADORES FATO'''
    table_name_project_inv_rep = 'public.FACT_PROJETO_INVESTIMENTO_REPASSADORES'
    key_columns_project_inv_rep  = ['sk_projeto_investimento', 'sk_repassador']
    crud_database_table(spark, sql_old_project_inv_rep, sql_new_project_inv_rep, table_name_project_inv_rep, key_columns_project_inv_rep, connection_properties, bulk_size)

    # CRUD fact_projeto_investimento_tipos
    sql_new_project_inv_tipos =     '''SELECT 	DISTINCT
                                                DPI.SK_PROJETO_INVESTIMENTO,
                                                DT.SK_TIPO
                                        FROM STG_PROJETO_INVESTIMENTO_TIPOS STG
                                        INNER JOIN DIM_TIPO DT ON DT.NK_TIPO = CAST(STG.ID AS VARCHAR) 
                                        INNER JOIN DIM_PROJETO_INVESTIMENTO DPI ON DPI.NK_PROJETO_INVESTIMENTO = STG.IDUNICO'''
    sql_old_project_inv_tipos =     '''SELECT 
                                                SK_PROJETO_INVESTIMENTO,
                                                SK_TIPO 
                                        FROM FACT_PROJETO_INVESTIMENTO_TIPOS FACT'''
    table_name_project_inv_tipos = 'public.FACT_PROJETO_INVESTIMENTO_TIPOS'
    key_columns_project_inv_tipos  = ['sk_projeto_investimento', 'sk_tipo']
    crud_database_table(spark, sql_old_project_inv_tipos, sql_new_project_inv_tipos, table_name_project_inv_tipos, key_columns_project_inv_tipos, connection_properties, bulk_size)

    # CRUD Fact_projeto_investimento_sub_tipos
    sql_new_project_inv_sub_tipos =     '''SELECT 	DISTINCT
                                                    DPI.SK_PROJETO_INVESTIMENTO,
                                                    DST.SK_SUB_TIPO
                                            FROM STG_PROJETO_INVESTIMENTO_SUB_TIPOS STG
                                            INNER JOIN DIM_SUB_TIPO DST ON DST.NK_SUB_TIPO = CAST(STG.ID AS VARCHAR) 
                                            INNER JOIN DIM_PROJETO_INVESTIMENTO DPI ON DPI.NK_PROJETO_INVESTIMENTO = STG.IDUNICO'''
    sql_old_project_inv_sub_tipos =     '''SELECT
                                                SK_PROJETO_INVESTIMENTO,
                                                SK_SUB_TIPO 
                                        FROM FACT_PROJETO_INVESTIMENTO_SUB_TIPOS FACT'''
    table_name_project_inv_sub_tipos = 'public.FACT_PROJETO_INVESTIMENTO_SUB_TIPOS'
    key_columns_project_inv_sub_tipos  = ['sk_projeto_investimento', 'sk_sub_tipo']
    crud_database_table(spark, sql_old_project_inv_sub_tipos, sql_new_project_inv_sub_tipos, table_name_project_inv_sub_tipos, key_columns_project_inv_sub_tipos, connection_properties, bulk_size)






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

@task(max_active_tis_per_dag=2)
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
        df = df.withColumn('nomeArquivo',regexp_replace(input_file_name(), '.*\/|\.json$', ''))
    else:
        raise Exception("File dosen't exist!")

    conn = jaydebeapi.connect(driver, url, [user_dw, password_dw], path_jdbc)
    curs = conn.cursor()
    curs.execute(f'''delete from stg_execucao_financeira where "nomeArquivo" = '{year}' ''')

    mode = 'append'
    properties = {"user": user_dw, "password": password_dw, "driver": driver}
    df.write.jdbc(url=url, table='stg_execucao_financeira', mode=mode, properties=properties)

@task(max_active_tis_per_dag=2)
def extract_json_projeto_investimento_date(dates, path):
    user_dw = os.getenv('USER_DW')
    password_dw = os.getenv('PASSWORD_DW')
    host_dw = os.getenv('HOST_DW')
    database_dw = os.getenv('DATABASE_DW')
    port_dw = os.getenv('PORT_DW')
    driver = os.getenv('DRIVER_JDBC_POSTGRES')
    path_jdbc = os.getenv('PATH_JDBC_POSTGRES')
    origin = os.getenv(path)
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
    
    df = spark.read.json(origins)
    df = df.withColumn('nomeArquivo',regexp_replace(input_file_name(), '.*\/|\.json$', ''))

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
    path = 'PATH_DEST_PROJETO_INVESTIMENTO_DATE'

    extract_api = extract_data_api_projecto_investimento_date.partial(url_base=url_base, endpoint=endpoint, page_size=page_size,
                             errors_limit = errors_limit, errors_consecutives_limit = errors_consecutives_limit, executions_limit = executions_limit).expand(date=dates)
    del_stgs = delete_stg_projeto_investimento(today, days)
    extract_json = extract_json_projeto_investimento_date(dates,path)

    extract_api >> extract_json
    del_stgs >> extract_json

@task_group()
def crud_projeto_investimento(bulk_size):
    crud_dim = crud_dimensions(bulk_size)
    del_dup_stg = delete_duplicates_stg()
    crud_fac = crud_facts(bulk_size)
    del_dup_stg >> crud_dim >> crud_fac

@task_group()
def extract_projeto_investimento_uf(url_base, endpoint, ufs, page_size, errors_limit, errors_consecutives_limit, executions_limit):

    path = 'PATH_DEST_PROJETO_INVESTIMENTO_UF'

    ufs_array = create_array(ufs)
    extract_api = extract_data_api_project.partial(url_base=url_base, endpoint=endpoint, page_size=page_size,
                             errors_limit = errors_limit, errors_consecutives_limit = errors_consecutives_limit, executions_limit = executions_limit).expand(uf=ufs_array)
    del_stgs = delete_stg_projeto_investimento_uf(ufs)
    extract_json = extract_json_projeto_investimento_date(ufs_array,path)
    #extract_json = extract_json_projeto_investimento_date(dates)

    extract_api >> extract_json
    del_stgs >> extract_json