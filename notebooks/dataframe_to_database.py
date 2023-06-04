import csv
from io import StringIO
import os
import pandas as pd
import time
from queue import Queue
import tempfile
from sqlalchemy import create_engine
import psycopg2


conn_string = 'postgresql://linpostgres:vJZKtc1tH4KE-Wp4@lin-22693-8146-pgsql-primary-private.servers.linodedb.net:5432/railway?sslmode=require'


def dataframe_to_database(dataframe: pd.DataFrame, table_name: str):
    engine = create_engine(conn_string)
    pg_conn = engine.connect()
    dataframe.to_sql(table_name, con=pg_conn, if_exists='append',
                     index=False, method='multi', chunksize=10000)


def copy_dataframe_to_database(dataframe: pd.DataFrame, table_name: str, pg_conn):
    with tempfile.TemporaryDirectory() as directory:
        file_path = os.path.join(directory, f'{table_name}.csv')
        dataframe.to_csv(file_path, index=False, header=False)

        with open(file_path, 'r') as f:
            with pg_conn.cursor() as cursor:

                cursor.copy_from(f, table_name, sep=',',
                                 columns=dataframe.columns)
                pg_conn.commit()


# # Example usage:
# input_string = "https://www.awebsite.com/path/to/some/file.txt"
# result = get_items_before_last_slash(input_string)


# This function takes a table name and drops it from a postgres database if it exists
def drop_table(table_name: str, pg_conn):
    with pg_conn.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        pg_conn.commit()


def connect_and_drop_tables(conn_string):
    # Table Names
    table_names = ['killmails', 'attackers', 'items', 'victims']
    # Establish a connection to the postgres database
    pg_conn = psycopg2.connect(conn_string)
    for table_name in table_names:
        drop_table(table_name, pg_conn)


def truly_copy_dataframe_to_database(dataframe: pd.DataFrame, table_name: str, pg_conn):
    # Database Connection string
    # print('Starting database upload or dataframe')
    # perform COPY test and print result
    now = time.time()
    csv_filename = f"{now}.csv"
    sql = f'''
    COPY {table_name}
    FROM '{csv_filename}' --input full file path here. see line 46
    DELIMITER ',' CSV;
    '''

    cur = pg_conn.cursor()
    # Truncate the table in case you've already run the script before
    cur.execute('TRUNCATE TABLE copy_test')

    # Name the .csv file reference in line 29 here
    dataframe.to_csv(csv_filename,
                     index=False, header=False)
    cur.execute(sql)
    pg_conn.commit()
    cur.close()


def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)


def no_truly_copy_dataframe_to_database(dataframe: pd.DataFrame, table_name: str, pg_conn):
    # engine = create_engine(conn_string)
    # pg_conn = engine.connect()
    dataframe.to_sql(table_name, con=pg_conn, if_exists='append',
                     index=False, method=psql_insert_copy, chunksize=10000)
    # engine.dispose()
