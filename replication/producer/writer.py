import msgpack
from kafka import KafkaProducer


class LogWriter:

    def __init__(self, host, port, key_serializer=str.encode, value_serializer=msgpack.dumps):
        self.producer = KafkaProducer(bootstrap_servers=f'{host}:{port}', key_serializer=key_serializer,
                                      value_serializer=value_serializer)

    def write(self, topic, value, **kwargs):
        return self.producer.send(topic=topic, value=value, **kwargs)

    def bulk_write(self, messages):
        responses = []
        for message in messages:
            meta_data = self.write(message['topic'], message['value'], key=message['key'])
            responses.append({'meta': meta_data, 'error': None})
        return responses

    def flush(self):
        self.producer.flush()

    def close(self):
        self.producer.close()


def get_log_writer(host, port, key_serializer, value_serializer):
    writer = None

    def init() -> LogWriter:
        nonlocal writer
        if not writer:
            writer = LogWriter(host, port, key_serializer, value_serializer)
        return writer

    return init
