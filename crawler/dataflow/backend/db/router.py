import time
import typing

from loguru import logger
from sqlalchemy import engine
from dataflow.backend.db import clients


def check_alive(connect: engine.base.Connection):
    connect.execute("SELECT 1 + 1")


def check_connect_alive(
    connect: engine.base.Connection,
    connect_func: typing.Callable,
):
    if connect:
        try:
            check_alive(connect)
            return connect
        except Exception as e:
            logger.info(
                f"""
                {connect_func.__name__} reconnect, error: {e}
                """
            )
            time.sleep(1)
            try:
                connect = connect_func()
            except Exception as e:
                logger.info(
                    f"""
                    {connect_func.__name__} connect error, error: {e}
                    """
                )
            return check_connect_alive(connect, connect_func)


class Router:
    def __init__(self):
        self._mysql_jobdata_conn = clients.get_mysql_jobdata_conn()

    def check_mysql_jobdata_conn_alive(self):
        self._mysql_jobdata_conn = check_connect_alive(
            self._mysql_jobdata_conn,
            clients.get_mysql_jobdata_conn,
        )
        return self._mysql_jobdata_conn

    @property
    def mysql_jobdata_conn(self):
        return self.check_mysql_jobdata_conn_alive()

    def close_connection(self):
        self._mysql_jobdata_conn.close()