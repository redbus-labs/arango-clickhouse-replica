from typing import Optional

# noinspection PyPackageRequirements
from arango import ArangoClient
# noinspection PyPackageRequirements
from arango.database import StandardDatabase
# noinspection PyPackageRequirements
from arango.wal import WAL

from arangodb.connect import ArangoDbConfig
from util.helper import singleton


class ArangoWAL:

    def __init__(self, config: ArangoDbConfig):
        self.config = config
        self.client: ArangoClient = ArangoClient(hosts=f'http://{config.host}:{config.port}')
        self.db: Optional[StandardDatabase] = None
        self.wal: Optional[WAL] = None

    def connect(self):
        self.db = self.client.db(name=self.config.db, username=self.config.username, password=self.config.password)
        self.wal = self.db.wal

    def get_last_tick(self):
        return self.wal.last_tick()

    def get_tick_ranges(self):
        return self.get_tick_ranges()


def get_arango_wal_client(db_config: ArangoDbConfig):
    aql_helper = ArangoWAL(db_config)
    aql_helper.connect()
    return aql_helper


@singleton
def get_singleton_wal_client(db_config: ArangoDbConfig):
    return get_arango_wal_client(db_config)
