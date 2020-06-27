# !/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Connector to generate connect, engine and session object
based on parameters defined in config.yaml
"""

__author__ = 'Laurent.Chen'
__date__ = '2019/7/15'
__version__ = '1.0.0'

import os
import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

with open(os.path.join(os.path.dirname(__file__), 'config.yaml'), 'r', encoding='utf-8') as f:
    config_dict = yaml.full_load(f)


class Connector(object):
    """
    MySQL connector

    :param str schema: name of target schema
    """

    host = config_dict['host']
    port = config_dict['port']
    user = config_dict['user']
    password = config_dict['password']
    charset = config_dict['charset']

    def __init__(self, schema):
        if not isinstance(schema, str):
            raise ValueError('schema must be str type')
        self.schema = schema

    def get_engine(self):
        """
        get SQLAlchemy engine

        :return: SQLAlchemy engine
        :rtype: sqlalchemy.engine.base.Engine
        """
        engine = create_engine(f"mysql+mysqldb://{self.user}:{self.password}@{self.host}:{self.port}/{self.schema}"
                               f"?charset=utf8mb4")
        return engine

    def get_connect(self):
        """
        get SQLAlchemy connect

        :return: SQLAlchemy connect
        :rtype: sqlalchemy.engine.base.Connection
        """
        conn = self.get_engine().connect()
        return conn

    def get_session(self):
        """
        get SQLAlchemy session

        :return: SQLAlchemy session
        """
        session = sessionmaker(bind=self.get_engine())
        return session()


if __name__ == '__main__':
    pass
