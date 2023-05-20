import asyncio
import json
import logging
import os
import time
import traceback

from django.core.serializers.json import DjangoJSONEncoder
from pytz import timezone

from components.helper import Helper


class Service:

    def __init__(self):
        """Constructor  for initializing necessary variables"""
        self.data = None
        self.helper = Helper()
        self.threads_list = []
        root_path = os.path.dirname(os.path.realpath(__file__))
        self.zone = timezone('Europe/Riga')
        logging.basicConfig(filename=root_path + '/warning_insertion.log', filemode='a')


    async def start(self):
        """This function starts data readings from database"""
        start_time = time.perf_counter()

        await self.helper.connect_to_db()

        try:
            warning_objs = await self.helper.fetch_warning_all()
            # returned all warning objectes , 312bytes(test)  ######

            for warning_obj in warning_objs:
                warning_obj = json.loads(json.dumps(dict(warning_obj), cls=DjangoJSONEncoder))
                # passed argument with reference type, id() ##### ram not loaded twice
                telem_obj = await self.helper.fetch_telem_data(warning_obj)
                if telem_obj:
                    # passed argument with refernce type, id() ##### ram not loaded
                    await self.helper.checker(warning_obj, telem_obj)

            # self.sql.close_connection()
            end_time = time.perf_counter()
            execution_time = end_time - start_time
            print(f"The execution time is: {execution_time}")
            await self.helper.close_connection()

        except BaseException:
            err = traceback.format_exc()
            logging.error(err)


# performance increase by 5-10 seconds in total depending on  size of data gets from table

async def init_app():
    app = Service()
    await app.start()




asyncio.run(init_app())

# import cProfile

# profiler = cProfile.Profile()
# profiler.enable()
#
# asyncio.run(init_app())
# profiler.disable()
# profiler.dump_stats("warning.stats")
# import pstats
# stats = pstats.Stats("warning.stats")
# stats.print_stats()
