# -*- coding: utf-8 -*-
import requests
import pandas as pd
from io import StringIO

# Функции (класс) для интеграции с ClickHouse
# Напишем функции для интеграции с ClickHouse: первая функция просто возвращает результат из DataBase, вторая же преобразует его в pandas DataFrame.
# Также напишем сразу удобную функцию для загрузки данных.

class simple_ch_client():
    def __init__(self, CH_HOST, CH_USER, CH_PASS, cacert):
        self.CH_HOST = CH_HOST
        self.CH_USER = CH_USER
        self.CH_PASS = CH_PASS
        self.cacert = cacert

    def get_version(self):
        url = '{host}/?database={db}&query={query}'.format(
                host=self.CH_HOST,
                db='default',
                query='SELECT version()')

        auth = {
                'X-ClickHouse-User': self.CH_USER,
                'X-ClickHouse-Key': self.CH_PASS,
            }

        rs = requests.get(url, headers=auth, verify=self.cacert)
        # 
        rs.raise_for_status()

        print(rs.text)

    def get_clickhouse_data(self, query, connection_timeout = 1500):
        r = requests.post(self.CH_HOST, params = {'query': query, 'user': self.CH_USER, 'password':self.CH_PASS}, timeout = connection_timeout, verify=self.cacert)
        if r.status_code == 200:
            return r.text
        else:
            raise ValueError(r.text)

    def get_clickhouse_df(self, query,connection_timeout = 1500):
        data = self.get_clickhouse_data(query, connection_timeout=connection_timeout) 
        df = pd.read_csv(StringIO(data), sep = '\t')
        return df

    def upload(self, table, content, data_format='TabSeparatedWithNames'):
        content = content.encode('utf-8')
        query_dict = {
                'query': 'INSERT INTO {table} FORMAT {data_format} '.format(table=table, data_format=data_format),
                'user': self.CH_USER, 
                'password':self.CH_PASS
            }
        r = requests.post(self.CH_HOST, data=content, params=query_dict, verify=self.cacert)
        result = r.text
        if r.status_code == 200:
            return result
        else:
            raise ValueError(r.text)

