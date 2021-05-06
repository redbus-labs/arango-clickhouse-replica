import os
import pathlib
from typing import Optional, List

import yaml

from clickhouse.connect import ClickhouseHelper
from util.basic_utils import get_basic_utilities, CONFIG, LOGGER
from util.common import get_supported_consumers
from util.helper import singleton


def load_schema_mapper(file_name) -> Optional[dict]:
    consumer_path = pathlib.Path(__file__).parent.parent.parent.absolute()
    mapper_path = os.path.join(consumer_path, 'tables', file_name)
    if os.path.exists(mapper_path):
        file = open(mapper_path, 'r')
        schema = yaml.safe_load(file)
        file.close()
        return schema
    return None


def get_table_map(table):
    config = get_basic_utilities().get(CONFIG)
    table_config = load_schema_mapper(f'{table}.yaml')
    schema = {
        'arango': table,
        'clickhouse': table_config['table_name'],
        'clickhouse_db': config['clickhouse']['database'],
        'table_create': table_config['table'],
        'schema': table_config['schema']
    }
    if 'buffer' in table_config:
        schema['buffer'] = table_config['buffer']
    if 'topic_config' in table_config:
        schema['topic_config'] = table_config['topic_config']
    return schema


@singleton
def get_schemas_map():
    schema_map = []
    for table in get_supported_consumers():
        schema = get_table_map(table)
        if schema:
            schema_map.append(schema)
    return schema_map


def get_table_map_by_clickhouse_table(ch_table) -> Optional[dict]:
    for table_map in get_schemas_map():
        if table_map['clickhouse'] == ch_table:
            return table_map
    return None


def get_table_map_by_arango_collection(collection) -> Optional[dict]:
    for table_map in get_schemas_map():
        if table_map['arango'] == collection:
            return table_map
    return None


def supported_arango_collections() -> List[str]:
    return [table_map['arango'] for table_map in get_schemas_map()]


def supported_clickhouse_collections() -> List[str]:
    return [table_map['clickhouse'] for table_map in get_schemas_map()]


def get_primary_key(consumer):
    table_map = get_table_map_by_arango_collection(consumer)
    schema = table_map['schema']
    return schema['primary_key']


def get_type_of_primary_key(ch_table):
    table_map = get_table_map_by_clickhouse_table(ch_table)
    properties = table_map['schema']['properties']
    primary_key = table_map['schema']['primary_key']
    for key in properties:
        if key == primary_key:
            return properties[key]['ch_type']
    return KeyError('primary key is not found')


def get_primary_key_map() -> dict:
    schema_primary_key_map = {}
    for table_map in get_schemas_map():
        clickhouse_table = table_map['clickhouse']
        schema_primary_key_map[clickhouse_table] = table_map['schema']['primary_key']
    return schema_primary_key_map


def create_buffer_table(client: ClickhouseHelper, db, table, table_map) -> bool:
    logging = get_basic_utilities().get(LOGGER)
    buffer_table = f'{db}.{table}_Buffer'
    if client.is_table_exists(buffer_table):
        return True
    # noinspection PyBroadException
    try:
        # noinspection SqlDialectInspection
        table_schema_query = '''
            SELECT create_table_query, engine_full
            FROM system.tables
            WHERE database = %(db)s AND name = %(table)s
        '''
        result = client.execute(query=table_schema_query, params={'db': db, 'table': table})
        schema, engine_details = result[0]
        schema = schema.replace(engine_details, '')
        schema = schema.replace(f'{db}.{table}', f'{buffer_table}')
        buffer = table_map['buffer']
        buffer_schema = f"{schema} Buffer({db}, {table}, {buffer['num_layers']}, {buffer['min_time']}, " \
                        f"{buffer['max_time']}, {buffer['min_rows']}, {buffer['max_rows']}, {buffer['min_bytes']}, " \
                        f"{buffer['max_bytes']})"
        client.execute(query=buffer_schema)
        return True
    except Exception as e:
        logging.error(f'failed to create to {buffer_table}: {e}', exc_info=True)
        return False
