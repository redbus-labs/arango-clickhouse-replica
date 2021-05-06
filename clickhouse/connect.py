from collections import namedtuple
from typing import Optional

from clickhouse_driver import Client

from util.helper import singleton

CHDbConfig = namedtuple('DbConfig', ['host', 'port', 'user', 'password', 'database'])


# noinspection SqlDialectInspection
class ClickhouseHelper:

    def __init__(self, config: CHDbConfig, settings=None):
        self.config = config
        self.client: Optional[Client] = None
        self.settings = settings
        if not self.settings:
            self.settings = {}

    def connect(self):
        self.client: Client = Client(host=self.config.host, port=self.config.port, database=self.config.database,
                                     user=self.config.user, password=self.config.password, settings=self.settings)

    def execute(self, query, params=None, **kwargs):
        return self.client.execute(query, params, **kwargs)

    def insert(self, document, table, columns=None):
        columns_str = ','.join(list(columns))
        insert_query = f'INSERT INTO {table} {"" if columns == "" else f"({columns_str})"} VALUES'
        return self.client.execute(insert_query, document)

    def bulk_insert(self, documents, table, columns=None, batch_size=10000):
        rows_inserted = 0
        for i in range(0, len(documents), batch_size):
            subset = documents[i:i + batch_size]
            rows_inserted += self.insert(subset, table, columns)
        return rows_inserted

    def insert_dict(self, document, table):
        columns = list(document.keys())
        columns_str = ','.join(list(columns))
        value = [document[col] for col in columns]
        insert_query = f'INSERT INTO {table} {"" if columns == "" else f"({columns_str})"} VALUES'
        return self.client.execute(insert_query, [value])

    def bulk_dict_doc_insert(self, documents, table, columns, batch_size=10000):
        documents_ordered = [[document[column] for column in columns] for document in documents]
        return self.bulk_insert(documents_ordered, table, columns, batch_size)

    def remove_doc_by_key(self, table, key, value):
        query = f'ALTER TABLE {table} WHERE {key}={value}'
        return self.client.execute(query)

    def optimize_table(self, table):
        query = f'OPTIMIZE TABLE {table} FINAL'
        return self.client.execute(query)

    def is_table_exists(self, table):
        result = self.client.execute(f'EXISTS TABLE {table}')
        return result[0][0] == 1

    def drop_table(self, table, settings=None):
        if settings is None:
            settings = {}
        return self.execute(f'DROP TABLE {table}', settings=settings)

    def drop_table_if_exists(self, table, settings=None):
        if settings is None:
            settings = {}
        return self.execute(f'DROP TABLE IF EXISTS {table}', settings=settings)

    def rename_table(self, old, new, settings=None):
        if settings is None:
            settings = {}
        return self.execute(f'RENAME TABLE {old} TO {new}', settings=settings)

    def drop_dictionary_if_exists(self, dictionary, settings=None):
        if settings is None:
            settings = {}
        return self.execute(f'DROP DICTIONARY IF EXISTS {dictionary}', settings)


def get_ch_client_with_dict_config(ch_config: dict):
    settings = ch_config['settings'] if 'settings' in ch_config else None
    client = ClickhouseHelper(CHDbConfig(
        host=ch_config['host'],
        port=ch_config['port'],
        user=ch_config['user'],
        password=ch_config['password'],
        database=ch_config['database'],
    ), settings)
    client.connect()
    return client


@singleton
def get_singleton_ch_client(ch_config):
    """
        note: connection retry mechanism is not required due to inbuilt auto retry support from driver
    """
    return get_ch_client_with_dict_config(ch_config)
