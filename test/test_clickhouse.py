from clickhouse.connect import ClickhouseHelper, get_singleton_ch_client, get_ch_client_with_dict_config
from replication.consumer.transformer import convert_to_ch_dict_using_schema
from replication.schema.helper import create_buffer_table, get_table_map
from util.basic_utils import CONFIG, get_basic_utilities
# noinspection PyUnresolvedReferences
from .test_basic_utils import basic_utilities


def test_clickhouse_connection(basic_utilities):
    config = basic_utilities.get(CONFIG)
    clickhouse: ClickhouseHelper = get_singleton_ch_client(config['clickhouse'])
    return clickhouse.execute('SELECT 1')


def create_test_table(table):
    config = get_basic_utilities().get(CONFIG)
    ch_client = get_ch_client_with_dict_config(config['clickhouse'])
    table_map = get_table_map(table)
    assert table_map is not None
    if ch_client.is_table_exists(f'{table_map["clickhouse"]}'):
        return True
    clickhouse_table = table_map['table_create']
    ch_client.execute(query=clickhouse_table)
    return True


def test_create_buffer_table(basic_utilities):
    table = 'test'
    table_buffer = f'{table}_Buffer'
    assert create_test_table(table)
    config = basic_utilities.get(CONFIG)
    ch_client = get_ch_client_with_dict_config(config['clickhouse'])
    table_map = get_table_map(table)
    assert table_map is not None
    if ch_client.is_table_exists(f'{table_buffer}'):
        ch_client.drop_table(table_buffer)
    assert create_buffer_table(ch_client, config['clickhouse']['database'], table_map['clickhouse'], table_map)


def test_none_values(basic_utilities):
    table = 'Test'
    assert create_test_table(table)
    config = basic_utilities.get(CONFIG)
    ch_client = get_ch_client_with_dict_config(config['clickhouse'])
    doc = {'_key': 1, 'name': 't1', 'email': 'test@email.com', 'Answers': '1,2,3'}
    table_map = get_table_map('test')
    clickhouse_doc = convert_to_ch_dict_using_schema(table_map['schema'], doc)
    result = ch_client.insert_dict(clickhouse_doc, table)
    assert result == 1
