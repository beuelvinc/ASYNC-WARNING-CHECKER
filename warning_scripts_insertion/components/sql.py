import logging
import os
import traceback
from datetime import datetime
from datetime import timedelta

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

    async def insert_warn_async(self, data):
        """
        @params :data: contains data of warning data merged by telem and warning table
        """
        try:
            date_now = datetime.now()
            telem_ts = data.get("telem_obj").get("ts")
            query = """INSERT INTO %s(warning_id, dp_id, abs_max, abs_min, telem_ts, telem_val, inserted_ts)
                                          VALUES(%s,%s,%s,%s,'%s',%s,'%s')""" % ((
                os.environ.get("warning_data_table_name"),
                data.get("warning_obj").get("id"),
                data.get("warning_obj").get("dp_id"),
                data.get("warning_obj").get("abs_max"),
                data.get("warning_obj").get("abs_min"),
                telem_ts,
                data.get("telem_obj").get("val"),
                date_now,
            ))
            await self.conn.execute(query)
            print("inserted")
        except BaseException:
            err = traceback.format_exc()
            logging.error(err)
            await self.reconnect()

    async def fetch_warning_all(self):
        """
        @params :conn: contains connection instance of database
        @return database objects in a list
         fetches  data from warning table in DB
        """
        try:
            query = """Select  * from %s  """ % (os.environ.get("warning_table_name"))
            return await self.conn.fetch(query)
        except BaseException:
            err = traceback.format_exc()
            logging.error(err)
            await self.reconnect()

    async def fetch_telem_data(self, obj):
        """
        @params :obj: warning instance of warning table
                :conn: contains connection instance of database
        @return the data of telem_data table
        """
        try:
            if obj.get("dp_id"):
                end = datetime.now(tz=self.zone)
                start = end - timedelta(minutes=20)
                ###### timedelta can be adjusted ######
                query = """SELECT *  FROM %s WHERE ts >= '%s' AND ts < '%s'  AND dp_id = %s""" % (
                    os.environ.get("telem_table_name"),
                    start, end, obj.get("dp_id"))
                rows = await self.conn.fetch(query)
                modified_rows = []
                for row in rows:
                    new_row = {}
                    new_row['id'] = row['id']
                    new_row['val'] = row['val']
                    new_row['ts'] = row['ts']
                    new_row['dp_id'] = row['dp_id']
                    new_row['ts'] = new_row['ts'].astimezone(self.zone)
                    modified_rows.append(new_row)

                return modified_rows
            else:
                return []
        except BaseException:
            err = traceback.format_exc()
            logging.error(err)
            await self.reconnect()
