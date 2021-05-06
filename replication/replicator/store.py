import traceback

import click
from clickhouse_driver import Client

from arangodb.connect import get_singleton_arango_client, ArangoHelper
from cache.connect import get_singleton_redis_client
from clickhouse.connect import ClickhouseHelper, get_singleton_ch_client
from replication.consumer.transformer import convert_to_ch_dict_using_schema
from replication.producer.reader import get_wal_client
from replication.schema.helper import get_table_map_by_arango_collection, create_buffer_table
from util.basic_utils import get_basic_utilities, CONFIG, LOGGER, SMTP_CLIENT
from util.common import get_supported_consumers


def get_all_documents(db_client: ArangoHelper, col_name, batch_size=10000):
    query = f'''
        for d in {col_name}
            return d
    '''
    for documents in db_client.select_using_yield(query, batch_size=batch_size, ttl=1800, options={'stream': True}):
        yield documents


def create_temporary_table(client: Client, create_command: str, table_name: str, temp_table_name: str):
    temp_table_create_command = create_command.replace(table_name, temp_table_name, 1)
    return temp_table_name, client.execute(temp_table_create_command)


def load_collection_data(collection, store_tick, batch_size):
    basic_utils = get_basic_utilities()
    config, logging, mail_client = basic_utils.get_utils((CONFIG, LOGGER, SMTP_CLIENT))

    arango_client = get_singleton_arango_client(config['arango'])

    clickhouse: ClickhouseHelper = get_singleton_ch_client(config['clickhouse'])
    clickhouse_client: Client = clickhouse.client

    clickhouse_table_map = get_table_map_by_arango_collection(collection)
    clickhouse_table, clickhouse_db, clickhouse_table_schema = (
        clickhouse_table_map['clickhouse'], clickhouse_table_map['clickhouse_db'], clickhouse_table_map['schema'])

    # prepare the tables
    clickhouse_temp_table = f'{clickhouse_table}Temp'
    clickhouse.drop_table_if_exists(f'{clickhouse_db}.{clickhouse_temp_table}')
    temp_table, table_created = create_temporary_table(clickhouse_client, clickhouse_table_map['table_create'],
                                                       f'{clickhouse_db}.{clickhouse_table}',
                                                       f'{clickhouse_db}.{clickhouse_temp_table}')
    logging.info(f'temporary table created for {clickhouse_table}')

    # store current tick for the table in redis
    if store_tick:
        wal_client = get_wal_client({**config['arango'], **config['wal']})
        last_tick = wal_client.get_last_tick()
        redis_config = config['redis']
        redis_helper = get_singleton_redis_client(redis_config['host'], redis_config['port'], redis_config['db'])
        redis_helper.client.set(f'{collection}:last-tick', last_tick['tick'])
        logging.info(f'stored current wal tick: {last_tick}')

    logging.info('collect documents from arango')
    processed_documents = 0
    errors = 0
    for documents in get_all_documents(db_client=arango_client, col_name=collection, batch_size=batch_size):
        logging.info(f'documents collected fom arango: {len(documents)} docs')

        # map the documents from arango to clickhouse document
        for i in range(len(documents)):
            try:
                documents[i] = convert_to_ch_dict_using_schema(clickhouse_table_schema, documents[i])
            except (TypeError, ValueError, KeyError):
                logging.document(f'doc: {documents[i]}')
                logging.document(f'error: {traceback.format_exc()}')
                documents[i] = None
                errors += 1

        # filter invalid documents
        documents = [doc for doc in documents if doc is not None]

        if len(documents) > 0:
            total_insertion = clickhouse.bulk_dict_doc_insert(documents, temp_table, list(documents[0].keys()),
                                                              batch_size)
            logging.info(f'populated data on clickhouse: {total_insertion} docs')
            processed_documents += total_insertion
        logging.info(f'overall processed documents: {processed_documents} docs')

    logging.info('data populated on temporary table')
    clickhouse.drop_table_if_exists(f'{clickhouse_db}.{clickhouse_table}')
    logging.info(f'dropped table {clickhouse_table}')
    clickhouse.rename_table(temp_table, f'{clickhouse_db}.{clickhouse_table}')
    logging.info('table populated successfully')
    logging.info(f'Incompatible documents: {errors}')

    # prepare buffer table
    if 'buffer' in clickhouse_table_map:
        clickhouse_buffer = f'{clickhouse_table}_Buffer'
        clickhouse.drop_table_if_exists(f'{clickhouse_db}.{clickhouse_buffer}')
        logging.info(f'dropped table {clickhouse_buffer}')
        create_buffer_table(clickhouse, clickhouse_db, clickhouse_table, clickhouse_table_map)
        logging.info('buffer table created successfully')

    return True


@click.command()
@click.option('--load-all', '-a', is_flag=True, help='load all supported tables')
@click.option('--exclude', '-e', help='comma separated list of tables to exclude from loading')
@click.option('--collections', '-c', help='Name of collection in Arango')
@click.option('--store-tick', '-st', is_flag=True, help='Store current tick in Redis')
@click.option('--batch-size', '-bs', default=10000)
def loader(load_all, exclude, collections, store_tick, batch_size):
    click.confirm('clickhouse table will be re-created with new data', abort=True)

    utils = get_basic_utilities()
    logging = utils.get(LOGGER)

    if not load_all and not collections:
        loader.error('inout not provided')
        return False

    enabled_consumers = get_supported_consumers()

    if load_all:
        all_collections = enabled_consumers
        collections = all_collections if exclude is None else [col for col in all_collections if
                                                               col not in exclude.split(',')]
    else:
        collections = collections.split(',')

    for collection in collections:
        # noinspection PyBroadException
        try:
            load_collection_data(collection, store_tick, batch_size)
        except Exception:
            logging.error(f'clickhouse {collection} loader failed: {traceback.format_exc()}')
            return False

    return True


if __name__ == '__main__':
    loader()
