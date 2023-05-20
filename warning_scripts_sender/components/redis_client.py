import logging
import os
import traceback

import redis


class RedisClient:
    """
    A class representing a Redis client connection.

    Attributes:
        redis_client (redis.Redis): The Redis client connection object.
    """

    def __init__(self):
        """
        Initializes a new Redis client connection.

        Uses environment variables to set the Redis host, port, and database number.
        """
        try:
            root_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

            logging.basicConfig(filename=root_path + '/warning_sender.log', filemode='a')

            self.redis_client = redis.Redis(host=os.getenv("redis_host"), port=int(os.getenv("redis_port")),
                                            db=int(os.getenv("redis_warning_db")))
        except BaseException as e:
            print(e)
            traceback.print_exc()

    def get_redis_client(self):
        if self.redis_client:
            return self.redis_client

    def read_all_data(self):
        keys = self.redis_client.keys()
        try:
            for key in keys:
                value = self.redis_client.get(key)
                self.redis_client.delete(key)
                # delete after reading , inn order to prevent loading redis too much
                yield value
        except Exception as e:
            err = traceback.format_exc()
            logging.error(err)
