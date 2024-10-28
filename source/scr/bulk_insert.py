from pyspark.sql.functions import col
from math import ceil
import  psycopg2 


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