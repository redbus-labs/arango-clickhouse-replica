import orjson
# noinspection PyPackageRequirements
from arango.wal import WAL

from arangodb.connect import ArangoDbConfig
from arangodb.wal import get_singleton_wal_client


class LogOpTypes:
    START_TRANSACTION = 2200
    COMMIT_TRANSACTION = 2201
    ABORT_TRANSACTION = 2202
    UPSERT_DOCUMENT = 2300
    REMOVE_DOCUMENT = 2302


def get_wal_client(config: dict):
    return get_singleton_wal_client(ArangoDbConfig(
        host=config['host'],
        port=config['port'],
        username=config['username'],
        password=config['password'],
        db=config['db']
    ))


def json_encode(obj):
    return orjson.dumps(obj)


def key_encode(key):
    if key is None:
        return None
    return str.encode(key)


class LogGenerator:

    def __init__(self, generator):
        self.generator = generator
        self.prev_processed = None
        self.limit_reached = False

    def __iter__(self):
        return self

    def __next__(self):
        if not self.limit_reached:
            data = self.generator.send(self.prev_processed)
            if self.prev_processed and not data['check_more']:
                self.limit_reached = True
            # ignore the last empty batch
            if int(data["last_included"]) == 0:
                raise StopIteration
            return data
        raise StopIteration

    def is_processed(self, value):
        self.prev_processed = value


def get_logs(client, tick_start, chunk_size=16384):
    wal: WAL = client.wal
    load = True
    while load:
        documents_info = wal.tail(lower=tick_start, deserialize=True, server_id=888, chunk_size=chunk_size)
        is_processed = yield documents_info
        if is_processed:
            load = documents_info['check_more']
            tick_start = documents_info['last_included']
