import asyncio
import json
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

            logging.basicConfig(filename=root_path + '/warning_insertion.log', filemode='a')

            self.redis_client = redis.Redis(host=os.getenv("redis_host"), port=int(os.getenv("redis_port")),
                                            db=int(os.getenv("redis_warning_db")))
        except BaseException as e:
            print(e)
            traceback.print_exc()

    async def save_into_redis(self, data):
        """
        this function saves data into redis  by key and value.
        A separate script will read data from this redis and delete data by key
        @params :data: contains the telem and warning instance of tables
        """
        try:
            await asyncio.get_running_loop().run_in_executor(None, self.redis_client.set,
                                                             str(data.get("warning_obj").get("id")) + "-" + str(
                                                                 data.get("telem_obj").get("id")),
                                                             json.dumps(data))

        except BaseException as e:
            err = traceback.format_exc()
            logging.error(err)
