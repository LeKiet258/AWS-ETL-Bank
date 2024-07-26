import pandas as pd
import psycopg2
from typing import Iterator, Dict, Any, Optional
import io
import time
from itertools import chain


class StringIteratorIO(io.TextIOBase):

    def __init__(self, iter: Iterator[str]):
        self._iter = iter
        self._buff = ''

    def readable(self) -> bool:
        return True

    def _read1(self, n: Optional[int] = None) -> str:
        while not self._buff:
            try:
                self._buff = next(self._iter)
            except StopIteration:
                break
        ret = self._buff[:n]
        self._buff = self._buff[len(ret):]
        return ret

    def read(self, n: Optional[int] = None) -> str:
        line = []
        if n is None or n < 0:
            while True:
                m = self._read1()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                m = self._read1(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)
        return ''.join(line)


def clean_csv_value(value: Optional[Any]) -> str:
    if value is None:
        return r'\N'
    return str(value).replace('\n', '\\n')


def create_connection():
    connection = psycopg2.connect(
        host="introduction-01-intro-ap-southeast-1-dev-introduction-db.cpfm8ml2cxp2.ap-southeast-1.rds.amazonaws.com",
        database="postgres",
        user="postgres",
        password="postgres123"
    )
    connection.set_session(autocommit=True)
    return connection


def create_tuple_str_data(data, data_field):
    return (data[field[0]] for field in data_field)


def is_table_exist(connection, table_name):
    cursor = connection.cursor()
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(table_name.replace('\'', '\'\'')))
    if cursor.fetchone()[0] == 1:
        return True

    return False


def create_db_if_exists(connection, table_name, data_field):
    if not is_table_exist(connection, table_name):
        cursor = connection.cursor()

        list_str_field_name_and_field_type = [f"{field[0]} {field[1]}" for field in data_field]

        result_field = ','.join(list_str_field_name_and_field_type)

        create_sql = f"""
            create table if not exists {table_name}(
                {result_field}
            );
        """

        cursor.execute(create_sql)
        time.sleep(1)
    else:
        print(f"Table {table_name} already exists")


def insert_db_using_sql(connection, table_name, data_field, list_data):
    cursor = connection.cursor()

    list_str_field_name = [f"{field[0]}" for field in data_field]

    data_insert = [
        f"""({idx} ,'{item['movie_link']}', '{item['poster_image_link']}','{item['movie_name']}','{item['movie_status']}','{item['en_name']}','{item['vn_name']}')"""
        for idx, item in enumerate(list_data)]

    result_field = ','.join(list_str_field_name)
    result_data = ','.join(data_insert)

    insert_sql = f"""
        insert into {table_name}({result_field})
        values
            {result_data};
    """

    print(insert_sql)

    cursor.execute(insert_sql)


def insert_db_using_copy_string_iterator(connection, tbl_name, data_field, iter_data: Iterator[Dict[str, Any]],
                                         size: int = 8192) -> None:
    with connection.cursor() as cursor:
        list_string_iterator = StringIteratorIO(
            ('|'.join(map(clean_csv_value, create_tuple_str_data(data, data_field))) + '\n' for data in iter_data)
        )

        cursor.copy_from(list_string_iterator, tbl_name, sep='|', size=size)


def fetch_data_by_sql(connection, query, data_field=None):
    cursor = connection.cursor()
    print('Db connection succesful')

    try:
        cursor.execute(query)
        columns = list(cursor.description)
        result = cursor.fetchall()
        print('Query executed succesfully')
    except (Exception, psycopg2.DatabaseError) as e:
        print(f"Exception: {e}")
        cursor.close()
        exit(1)

    cursor.close()

    # change result to dict
    results = []
    for row in result:
        row_dict = {}
        for i, col in enumerate(columns):
            row_dict[col.name] = row[i]
        results.append(row_dict)

    return results


def create_binary_from_list_data(iter_data: Iterator[Dict[str, Any]]):
    list_string_iterator = StringIteratorIO(
        chain(
            ','.join(map(clean_csv_value, tuple(iter_data[0].keys()))) + '\n',
            (','.join(map(clean_csv_value, tuple(data.values()))) + '\n' for data in iter_data)
        )
    )
    return list_string_iterator

def get_data_fetchmany(cursor, tbl_name):
    # get column name
    cursor.execute(f'''
        select column_name from information_schema.columns
        where table_schema = 'ai4e_test'
        and table_name = '{tbl_name}'
    ''')
    cols = cursor.fetchall()
    col_ls = [col[0] for col in cols]

    cursor.execute(f'''SELECT * from ai4e_test.{tbl_name}''')
    result = cursor.fetchmany(size=1000)

    df = pd.DataFrame(result, columns=col_ls)

    return df
