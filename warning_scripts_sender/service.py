import asyncio
import json
import logging
import os
import traceback

from pytz import timezone

from components.celery_client import send_your_email
from components.redis_client import RedisClient
from components.sql import SQL


class Service:

    def __init__(self):
        """Constructor  for initializing necessary variables"""
        self.zone = timezone('Europe/Riga')
        root_path = os.path.dirname(os.path.realpath(__file__))
        logging.basicConfig(filename=root_path + '/warning_sender.log', filemode='a')
        self.redis_client = RedisClient()

        self.sql = SQL()

    async def create_email_text(self, warning_data, telem_data, unit):
        """
        @params :warning_data: dict, is the data taken from redis that contains warning instance
        @params :telem_data: dict, is the data taken from redis that contains warning instance
        @params :unit: str, represents unit of datapoint

        @return str, descriptive text for emails
        """

        txt = ""
        txt += warning_data.get("email_text")
        txt += "\n Apraksts: "
        txt += warning_data.get("warn_descriptive_name")

        txt += "\n Normālais vērtības diapazons ir no {} - {} {} , datu punkta vērtība ir  {} {}".format(
            warning_data.get("abs_min"),
            warning_data.get("abs_max"),
            unit,
            telem_data.get("val"),
            unit)

        txt += " plkst.{}".format(telem_data.get("ts").replace("T", " ").replace("Z", " "))

        return txt

    async def create_email_text_admin(self, normal_text, warning_id):
        """
        @params :normal_text: str, email description
        @params :warning_id: int, warning id in table
        @return str, descriptive text for emails for admins who have no email assigned
        """

        normal_text += "\n this email is sent to you  because warning_id {} has no email assigned".format(warning_id)
        return normal_text

    async def start(self):
        try:
            await self.sql.connect_to_db()

            for value in self.redis_client.read_all_data():
                value = json.loads(value)
                email = await self.sql.fetch_email(value.get("warning_obj").get("id"))
                unit = await self.sql.fetch_dp_unit(value.get("warning_obj").get("dp_id"))
                text = await self.create_email_text(value.get("warning_obj"), value.get("telem_obj"), unit)

                if not email:
                    text = await self.create_email_text_admin(text, value.get("warning_obj").get("id"))
                    email = "demo@gmail.com"

                send_your_email.delay(text, email)

        except BaseException as e:
            err = traceback.format_exc()
            logging.error(err)


async def init_app():
    app = Service()
    await app.start()


asyncio.run(init_app())
