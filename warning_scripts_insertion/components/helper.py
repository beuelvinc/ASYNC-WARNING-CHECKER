import asyncio
import json
import logging
import os
import traceback

from components.redis_client import RedisClient
from components.sql import SQL
from django.core.serializers.json import DjangoJSONEncoder
from pytz import timezone


class Helper:
    """
    Helper codes class
    """

    def __init__(self):
        try:
            root_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
            logging.basicConfig(filename=root_path + '/warning_insertion.log', filemode='a')
            self.redis_cli = RedisClient()
            self.zone = timezone('Europe/Riga')

        except Exception as e:
            err = traceback.format_exc()
            logging.error(err)

    async def checker(self, warning_obj, telem_objs):
        """
        @info The private method is aimed to check all necessary procedure
        @params :warning_obj: warning instance of the table.
        @params :telem_objs:  telem_data instances of the table.

        @return
        """

        try:
            tasks = []
            # with ThreadPoolExecutor(max_workers=2) as executor: ## increases the performance by 4 seconds
            #     for obj in telem_objs:
            #         if float(warning_obj['abs_max']) < float(obj['val']) or float(warning_obj['abs_min']) > float(
            #                 obj['val']):
            #             merged_data = {"warning_obj": warning_obj, "telem_obj": obj}
            #             tasks.append(self.thread_help.save_into_redis(merged_data))
            #             executor.submit(self.sql.insert_to_warning_data, merged_data)

            for obj in telem_objs:
                obj = json.loads(json.dumps(dict(obj), cls=DjangoJSONEncoder))
                if float(warning_obj['abs_max']) < float(obj['val']) or float(warning_obj['abs_min']) > float(
                        obj['val']):
                    merged_data = {"warning_obj": warning_obj, "telem_obj": obj}
                    tasks.append(self.redis_cli.save_into_redis(merged_data))

                    #######  increases the performance by 8 seconds #######
                    await self.sql.insert_warn_async(merged_data)
            await asyncio.gather(*tasks)

        except BaseException as e:
            err = traceback.format_exc()
            logging.error(err)

    async def fetch_warning_all(self):
        """Helper func for sql class"""

        return await self.sql.fetch_warning_all()

    async def fetch_telem_data(self, warning_obj):
        """Helper func for sql class"""
        return await self.sql.fetch_telem_data(warning_obj)

    async def connect_to_db(self):
        """Helper func for sql class"""
        self.sql = SQL()
        await self.sql.connect_to_db()

    async def close_connection(self):
        """Helper func for sql class"""
        await self.sql.close_db()
