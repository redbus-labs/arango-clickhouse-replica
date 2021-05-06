from pyArango.collection import Collection

from cache.connect import get_singleton_redis_client
from replication.producer.publisher import set_tick_if_not_set, collect_logs, get_arango_collections
from replication.producer.reader import get_wal_client, LogGenerator
from util.basic_utils import CONFIG
# noinspection PyUnresolvedReferences
from .test_arango import temp_empty_table
# noinspection PyUnresolvedReferences
from .test_basic_utils import basic_utilities


def test_set_tick_if_not_set(basic_utilities):
    config = basic_utilities.get(CONFIG)
    arango_wal_client = get_wal_client({**config['arango'], **config['wal']})
    redis_helper = get_singleton_redis_client(config['redis']['host'], config['redis']['port'], config['redis']['db'])
    deleted = redis_helper.client.delete('last-tick')
    tick = redis_helper.client.get('last-tick')
    assert deleted and tick is None
    set_tick = set_tick_if_not_set(arango_wal_client, redis_helper)
    tick = redis_helper.client.get('last-tick')
    assert set_tick and tick is not None
    set_tick = set_tick_if_not_set(arango_wal_client, redis_helper)
    tick = redis_helper.client.get('last-tick')
    assert not set_tick and tick is not None


def test_log_generator_retry(basic_utilities, temp_empty_table: Collection):
    assert temp_empty_table is not None
    config = basic_utilities.get(CONFIG)
    arango_wal_client = get_wal_client({**config['arango'], **config['wal']})
    last_tick = str(arango_wal_client.get_last_tick()['tick'])
    temp_docs = [
        {'name': 't1', 'attr1': 1, 'attr2': 1},
        {'name': 't2', 'attr1': 2, 'attr2': 2},
        {'name': 't3', 'attr1': 3, 'attr2': 3}
    ]
    temp_empty_table.importBulk(data=temp_docs)
    arango_collections = get_arango_collections(config['arango'])
    collections_id_dict = {collection: meta.globallyUniqueId for collection, meta in arango_collections.items() if
                           collection in ['test']}
    logs_collector = collect_logs(arango_wal_client, last_tick, None, collections_id_dict)
    logs_generator = LogGenerator(logs_collector)
    t1 = []
    for docs in logs_generator:
        t1.extend(docs['content'])
        logs_generator.is_processed(False)
        break
    t2 = []
    for docs in logs_generator:
        t2.extend(docs['content'])
        logs_generator.is_processed(True)
        break
    t3 = []
    for docs in logs_generator:
        t3.extend(docs['content'])
        logs_generator.is_processed(True)
    return t1 == t2 and len(t3) < 1
