import pandas as pd

import time
from queue import Queue
import tempfile


def dataframe_to_database(dataframe: pd.DataFrame, table_name: str, pg_conn):
    # Database Connection string
    # print('Starting database upload or dataframe')
    # with tempfile.TemporaryDirectory() as tmpdirname:
    #     # perform COPY test and print result
    #     csv_filename = (tmpdirname + '/temp.csv')
    #     sql = f'''
    #     COPY copy_test
    #     FROM '{csv_filename}' --input full file path here. see line 46
    #     DELIMITER ',' CSV;
    #     '''

    #     table_create_sql = '''
    #     CREATE TABLE IF NOT EXISTS copy_test (id                bigint,
    #                                         quantity          int,
    #                                         cost              double precision,
    #                                         total_revenue     double precision)
    #     '''

    #     cur = pg_conn.cursor()
    #     cur.execute(table_create_sql)
    #     # Truncate the table in case you've already run the script before
    #     cur.execute('TRUNCATE TABLE copy_test')

    #     # Name the .csv file reference in line 29 here
    #     dataframe.to_csv(csv_filename,
    #                      index=False, header=False)
    #     cur.execute(sql)
    #     pg_conn.commit()
    #     cur.close()
    dataframe.to_sql(table_name, con=pg_conn, if_exists='append',
                     index=True, )

# # Example usage:
# input_string = "https://www.awebsite.com/path/to/some/file.txt"
# result = get_items_before_last_slash(input_string)
# print(result)
