from typing import Optional

import redis

from util.helper import singleton


class RedisHelper:

    def __init__(self, host, port, db):
        self.host = host
        self.port = port
        self.db = db
        self.client: Optional[redis.Redis] = None

    def connect(self):
        self.client = redis.Redis(host=self.host, port=self.port, db=self.db)

    def is_connected(self):
        try:
            self.client.ping()
            return True
        except ConnectionError:
            return False

    def publish(self, channel, message):
        return self.client.publish(channel, message)

    def subscribe(self, pattern, poll_interval=.01):
        pubsub = self.client.pubsub()
        pubsub.psubscribe(**pattern)
        thread = pubsub.run_in_thread(sleep_time=poll_interval)
        return thread


def get_redis_client(host, port, db):
    redis_helper = RedisHelper(host, port, db)
    redis_helper.connect()
    return redis_helper


@singleton
def get_singleton_redis_client(host, port, db):
    return get_redis_client(host, port, db)
