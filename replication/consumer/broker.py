import orjson
# noinspection PyPackageRequirements,PyProtectedMember
from kafka import KafkaConsumer


def connect_consumer(**kwargs):
    return KafkaConsumer(**kwargs)


def json_decode(obj):
    return orjson.loads(obj.decode('utf-8'))


def custom_connect_consumer(host, port, topic, group):
    return KafkaConsumer(
        topic,
        group_id=group,
        bootstrap_servers=f'{host}:{port}',
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=json_decode,
    )


def all_messages_consumed(consumer: KafkaConsumer):
    partitions = list(consumer.assignment())
    if len(partitions) < 1:
        return False
    last_commit = consumer.committed(partitions[0])
    last_offset = consumer.end_offsets(partitions=partitions)
    last_offset = list(last_offset.values())[0]
    return last_commit == last_offset
