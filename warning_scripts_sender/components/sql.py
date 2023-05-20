import logging
import os
import traceback

import asyncpg
from pytz import timezone


class SQL:
    """
    A class representing a SQL database connection.
    """

    def __init__(self):
        """
        Initializes a new SQL object.
        """
        try:
            root_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
            logging.basicConfig(filename=root_path + '/warning_insertion.log', filemode='a')
            self.zone = timezone('Europe/Riga')

        except Exception as e:
            err = traceback.format_exc()
            logging.error(err)

    async def connect_to_db(self):
        """Creates connection to DB based on env variables"""
        try:
            self.conn = await asyncpg.connect(
                # async database connection ###
                user=os.environ.get("DB_un"),
                password=os.environ.get("DB_pw"),
                host=os.environ.get("DB_host"),
                port=os.environ.get("DB_port"),
                database=os.environ.get("DB_name"))
        except BaseException:
            err = traceback.format_exc()
            logging.error(err)
            await self.reconnect()

    async def reconnect(self):
        """
         Reconnect to the database.

         Closes the existing database connection and creates a new connection to the same database.
         """
        try:
            await self.conn.close()
            await self.conn.connect_to_db()
        except BaseException:
            err = traceback.format_exc()
            logging.error(err)

    async def close_db(self):
        """Closes the connection to the database. """
        try:
            await self.conn.close()
        except BaseException:
            err = traceback.format_exc()
            logging.error(err)

    async def fetch_email(self, id):
        """
        fetches email  by given id form database
        @param :id: int, warning ID
        @return user email or None
        """
        try:
            query = """SELECT email  FROM %s u INNER JOIN %s w ON u.id = w.user_id WHERE w.warning_id = %s;""" % (
                id, os.environ.get("users_table_name"), os.environ.get("warning_user_table_name"))
            email = await self.conn.fetch(query)
            if len(email) > 0:
                return email[0].get("email")
            else:
                return None


        except BaseException:
            err = traceback.format_exc()
            logging.error(err)
            await self.reconnect()

    async def fetch_dp_unit(self, dp_id):
        """
        fetches datapoint unit  by given datappoint id form datapoints table
        @param :dp_id: int, datapoit ID
        @return unit or None
        """
        try:
            query = """SELECT *  FROM  %s WHERE id = %s;""" % (os.environ.get("datapoint_table_name"),
                                                               dp_id)
            dp_id = await self.conn.fetch(query)
            if len(dp_id) > 0:
                return dp_id[0].get("dp_units")
            else:
                return None


        except BaseException:
            err = traceback.format_exc()
            logging.error(err)
            await self.reconnect()

    async def fetch_warning(self, warn_id):
        """
        fetches warning  by given id form warning table
        @param :warn_id: int, warning ID
        """
        try:
            query = """Select  * from %s   where id=%s""" % (os.environ.get("warning_table_name"), warn_id)
            return await self.conn.fetch(query)

        except BaseException:
            err = traceback.format_exc()
            logging.error(err)
            await self.reconnect()
