import time
import json
from pathlib import Path
import requests
import json
from airflow.decorators import dag, task, task_group
from airflow.models.param import Param
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, substring, explode, to_date, col, regexp_replace
from pyspark.sql.types import ArrayType
import os.path 
import os
from dotenv import load_dotenv
from functools import reduce
import  psycopg2 
from math import ceil

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
        if errors_consecutives > errors_consecutives_limit:
            raise Exception("Number of consecutives errors exceeded")
        elif errors > errors_limit:
            raise Exception("Number of total errors exceeded")
        elif executions > executions_limit:
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
        time.sleep(1 + errors_consecutives)
        #if response.status_code == 429:
            #time.sleep(1 + errors_consecutives)
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
        time.sleep(1 + errors_consecutives)
        #if response.status_code == 429:
        #    time.sleep(1 + errors_consecutives)
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
        time.sleep(1 + errors_consecutives)
        #if response.status_code == 429:
        #    time.sleep(1 + errors_consecutives)
        return success, None,  page, errors_consecutives, errors, executions, response.status_code
    
def values_to_update(df_old, df_new, key_columns, compare_columns):
    """
    Compared two dataframes and find the rows to update.

    :param df_old: Old DataFrame
    :param df_new: New DataFrame
    :param key_columns: Join columns
    :param compare_columns: Comparisson coluns
    :return: Update DataFrame
    """
    # Criar a condição de join
    join_condition = [col(f"new.{key}") == col(f"old.{key}") for key in key_columns]
    
    # Realizar o join
    joined_df = df_new.alias("new").join(df_old.alias("old"), on=join_condition, how="inner")

    # Filtrar as diferenças
    # filter_condition = [
    #    (col(f"new.{column}") != col(f"old.{column}")) for column in compare_columns
    #]

    filter_condition = [
        f'not new.{column} <=> old.{column}' for column in compare_columns
    ]

    # Aplicar o filtro
    #differences = joined_df.filter(reduce(lambda a, b: a | b, filter_condition))
    if len(filter_condition) == 0:
        differences = joined_df.filter('0 = 1')
    else:     
        differences = joined_df.filter(' or '.join(filter_condition))
    
    return differences.select("new.*")

def values_to_insert_delete(df1, df2, key_columns):
    """
    Compared two dataframes and find the rows to update.

    :param df1: Secondary DataFrame
    :param df2: Main DataFrame
    :param key_columns: Join columns
    :return: Insert Or Delete DataFrame
    """
    # Criar a condição de join
    join_condition = [col(f"new.{key}") == col(f"old.{key}") for key in key_columns]
    
    # Realizar o join
    joined_df = df2.alias("new").join(df1.alias("old"), on=join_condition, how="leftanti")

    return joined_df.select("new.*")

def create_sql(table_name, columns, key_columns, type):
    sql = ''
    if type in ('DELETE'):
        columns_comparisson = [f'"{column}" = %s' for column in key_columns]
        columns_comparisson = ' and '.join(columns_comparisson)
        sql = f'DELETE FROM {table_name} WHERE {columns_comparisson}'
    elif type in ('INSERT'):
        columns_str = list(map(lambda x: '"' + x + '"',columns + key_columns))
        columns_str = ', '.join(columns_str)
        placeholders = ', '.join(['%s'] * len(columns + key_columns))
        sql = f'INSERT INTO {table_name}({columns_str}) VALUES({placeholders})'
    elif type in ('UPDATE'):
        columns_comparisson = [f'"{column}" = %s' for column in key_columns]
        columns_comparisson = ' and '.join(columns_comparisson)
        columns_set = [f'"{column}" = %s' for column in columns]
        columns_set = ', '.join(columns_set)
        sql = f'UPDATE {table_name} SET {columns_set} WHERE {columns_comparisson}'

    return sql

def create_connection(connection_properties): 
    #Connect to the Postgresql database using the psycopg2 adapter. 
    #Pass your database name , username , password , hostname and port number 
    conn = psycopg2.connect(f"dbname='{connection_properties['db_name']}' user='{connection_properties['user']}' password='{connection_properties['password']}'\
                            host='{connection_properties['host']}' port='{connection_properties['port']}'") 
    #Get the cursor object from the connection object 
    curr = conn.cursor() 
    return conn,curr 

def bulk_values(table_name, df, bulk_size, connection_properties, type, columns, key_columns=[]):

    if df.count() == 0:
        return

    splits = ceil(df.count() / bulk_size)
    conn, curr = create_connection(connection_properties)
    rdn_splits = [float(bulk_size)] * splits
    splits_dfs = df.randomSplit(rdn_splits)


    for split_index, split_df in enumerate(splits_dfs):

        if split_df.count() == 0:
            continue

        query = create_sql(table_name, columns, key_columns, type)

        if type in ('UPDATE', 'INSERT'):
            split_df = split_df.select(columns + key_columns)
        elif type in ('DELETE'):
            split_df = split_df.select(key_columns)


        rdd = split_df.rdd
        data = rdd.map(tuple)
        data = data.collect()


        curr.executemany(query, data) 
        conn.commit() 

        print(f'Split: {split_index + 1}\\{splits}\
                Bulk Size: {split_df.count()}')
        split_df.unpersist()

    conn.close() 

def crud_database(df_old, df_new, table_name, key_columns, connection_properties, bulk_size, insert = True, update = True, delete = False):
    columns_comparisson = df_new.columns
    columns_comparisson = [column for column in columns_comparisson if not column in key_columns] 
    print(f'CRUD starting for table {table_name}')

    if delete == True:
        df_delete = values_to_insert_delete(df_new, df_old, key_columns)
        print('Starting delete...')
        bulk_values(table_name, df_delete, bulk_size, connection_properties, 'DELETE' ,columns_comparisson, key_columns)
        print(f'Deleted {df_delete.count()} rows')
        df_delete.unpersist()

    if update == True:
        df_update = values_to_update(df_old, df_new, key_columns, columns_comparisson)
        print('Starting update...')
        bulk_values(table_name, df_update, bulk_size, connection_properties, 'UPDATE' ,columns_comparisson, key_columns)
        print(f'Updated {df_update.count()} rows')
        df_update.unpersist()

    if insert == True:
        df_insert = values_to_insert_delete(df_old, df_new, key_columns)
        print('Starting insert...')
        bulk_values(table_name, df_insert, bulk_size, connection_properties, 'INSERT' ,columns_comparisson, key_columns)
        print(f'Inserted {df_insert.count()} rows')
        df_insert.unpersist()

def crud_database_table(spark, sql_old, sql_new, table_name, key_columns, connection_properties, bulk_size, insert = True, update = True, delete = False):

    url = f'jdbc:postgresql://{connection_properties['host']}:{connection_properties['port']}/{connection_properties['db_name']}'

    df_new = spark.read.format("jdbc")\
        .option("url", url)\
        .option("query", sql_new)\
        .option("driver", connection_properties['driver'])\
        .option("user", connection_properties['user'])\
        .option("password", connection_properties['password'])\
        .load().cache()

    df_old = spark.read.format("jdbc")\
        .option("url", url)\
        .option("query", sql_old)\
        .option("driver", connection_properties['driver'])\
        .option("user", connection_properties['user'])\
        .option("password", connection_properties['password'])\
        .load().cache()
    
    crud_database(df_old, df_new, table_name, key_columns, connection_properties, bulk_size, insert, update, delete)

    df_new.unpersist()
    df_old.unpersist()

def sql_delete_duplicates(table_name, key_columns):
    select_key_columns = ' , '.join(key_columns)
    comparison_keys_columns = [f'a.{key_column} = b.{key_column}' for key_column in key_columns]
    where_key_columns = '\nAND '.join(comparison_keys_columns)
    sql = f'''DELETE FROM {table_name} a USING (
                SELECT MIN(ctid) as ctid, {select_key_columns}
                FROM {table_name} 
                GROUP BY ({select_key_columns}) HAVING COUNT(*) > 1
            ) b
            WHERE {where_key_columns}
            AND a.ctid <> b.ctid'''
    return sql

def delete_duplicates(table_name, key_columns, connection_properties):
    conn, curr = create_connection(connection_properties)
    sql_delete = sql_delete_duplicates(table_name, key_columns)

    curr.execute(sql_delete)

    conn.commit()
    conn.close()

def delete_duplicates_distinct_years(table_name, years, connection_properties):
    conn, curr = create_connection(connection_properties)

    curr.execute(f'''create table IF NOT EXISTS {table_name}_temp (like {table_name})''')
    conn.commit()

    curr.execute(f'''INSERT INTO {table_name}_temp SELECT DISTINCT * FROM {table_name} where cast("nomeArquivo" as integer) in ({','.join(years)})''')
    conn.commit()

    curr.execute(f'''delete from {table_name} where cast("nomeArquivo" as integer) in ({','.join(years)})''')
    conn.commit()

    curr.execute(f'''insert into {table_name} select * from {table_name}_temp''')
    conn.commit()

    curr.execute(f'''drop table {table_name}_temp''')
    conn.commit()

    conn.close()

def delete_duplicates_distinct(table_name, connection_properties):
    conn, curr = create_connection(connection_properties)

    curr.execute(f'''create table IF NOT EXISTS {table_name}_temp (like {table_name})''')
    conn.commit()

    curr.execute(f'''INSERT INTO {table_name}_temp SELECT DISTINCT * FROM {table_name}''')
    conn.commit()

    curr.execute(f'''delete from {table_name}''')
    conn.commit()

    curr.execute(f'''insert into {table_name} select * from {table_name}_temp''')
    conn.commit()

    curr.execute(f'''drop table {table_name}_temp''')
    conn.commit()

    conn.close()