from collections import namedtuple
from typing import Optional

# noinspection PyPackageRequirements
from pyArango.connection import Connection
from pyArango.database import Database

from util.helper import singleton

ArangoDbConfig = namedtuple('DbConfig', ['host', 'port', 'username', 'password', 'db'])


class ArangoHelper:

    def __init__(self, config: ArangoDbConfig):
        self.config = config
        self._connection = None
        self.db: Optional[Database] = None

    def connect(self):
        self._connection: Connection = Connection(
            arangoURL=f'http://{self.config.host}:{self.config.port}',
            username=self.config.username,
            password=self.config.password
        )
        self.db: Database = self._connection[self.config.db]

    def execute(self, query, params=None):
        if params is None:
            params = {}
        return self.db.AQLQuery(query=query, bindVars=params)

    def select(self, query, params=None, batch_size=10000, raw_results=True, **kwargs):
        if params is None:
            params = {}
        result_set = self.db.AQLQuery(query=query, bindVars=params, rawResults=raw_results, batchSize=batch_size,
                                      **kwargs)
        documents = []
        while True:
            try:
                documents.extend(result_set.response['result'])
                result_set.nextBatch()
            except StopIteration:
                break
        return documents

    def select_using_yield(self, query, params=None, batch_size=10000, raw_results=True, **kwargs):
        if params is None:
            params = {}
        result_set = self.db.AQLQuery(query=query, bindVars=params, rawResults=raw_results, batchSize=batch_size,
                                      **kwargs)
        while True:
            try:
                yield result_set.response['result']
                result_set.nextBatch()
            except StopIteration:
                break

    def select_one(self, query, params=None, raw_results=True, **args):
        if params is None:
            params = {}
        result_set = self.db.AQLQuery(query=query, bindVars=params, rawResults=raw_results, **args)
        return result_set.response['result'][0]


def get_arango_client_using_dict_config(arango_config: dict):
    client = ArangoHelper(ArangoDbConfig(
        host=arango_config['host'],
        port=arango_config['port'],
        username=arango_config['username'],
        password=arango_config['password'],
        db=arango_config['db']
    ))
    client.connect()
    return client


@singleton
def get_singleton_arango_client(arango_config: dict):
    return get_arango_client_using_dict_config(arango_config)
