import pytest

from arangodb.connect import get_singleton_arango_client
from clickhouse.connect import get_singleton_ch_client, ClickhouseHelper
from util.basic_utils import get_basic_utilities, CONFIG


@pytest.fixture(scope='package', autouse=True)
def cleanup():
    yield None
    config = get_basic_utilities().get(CONFIG)
    arango_client = get_singleton_arango_client(config['arango'])
    collection = 'test'
    collection_exists = collection in arango_client.db.collections
    if collection_exists:
        arango_client.db.collections[collection].delete()
        del arango_client.db.collections[collection]
    clickhouse: ClickhouseHelper = get_singleton_ch_client(config['clickhouse'])
    clickhouse.drop_table_if_exists(collection)
    clickhouse.drop_table_if_exists(f'{collection}_Buffer')
