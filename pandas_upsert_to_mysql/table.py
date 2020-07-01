# !/usr/bin/env python
# -*- coding: utf-8 -*-

"""
MySQL table structure

Declare table using SQLAlchemy ORM

Normally each table should have three classes::
    - abstract table
    - temp table
    - the exact table

We use only the auto_incremented row_id as primary key here,
while column(s) we use to judge whether to update or not are
set as unique constraint.

Columns in unique constraint should not have any null values,
otherwise it would cause unexpected duplicates.
see https://bugs.mysql.com/bug.php?id=8173

The 'update_time' field here uses one of MySQL's features,
treating the first timestamp field with param 'nullable=False'
as 'DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'.
Therefore, the definition order of 'update_time' and
'create_time' should not be exchanged.
"""
import pandas as pd
from sqlalchemy import (Index, Column, Integer, String, TIMESTAMP, func, text)
from sqlalchemy.ext.declarative import declarative_base

from connection import Connector

Base = declarative_base()


class AbstractOrder(Base):
    """
    Abstract order table
    """

    __abstract__ = True

    row_id = Column(Integer, autoincrement=True, primary_key=True,
                    comment='auto_incremented_ID')  #: auto_incremented_id
    order_id = Column(String(5), nullable=False, server_default='-9999', comment='order_id')  #: order_id
    product_id = Column(String(5), nullable=False, server_default='-9999', comment='product_id')  #: product_id
    qty = Column(Integer, comment='purchase_quantity')  #: purchase_quantity
    refund_qty = Column(Integer, comment='refund_quantity')  #: refund_quantity

    __table_args__ = (Index('main', 'order_id', 'product_id', unique=True),
                      {'comment': 'Order Info'})


class Order(AbstractOrder):
    """
    Order table
    """
    __tablename__ = 'order_info'

    update_time = Column(TIMESTAMP, nullable=False,
                         server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'),
                         comment='last_update_time')
    create_time = Column(TIMESTAMP, nullable=False, server_default=func.current_timestamp(),
                         comment='first_create_time')

    __table_args__ = (Index('main', 'order_id', 'product_id', unique=True),
                      {'comment': 'Order Info'})


class OrderTemp(AbstractOrder):
    """
    Temp order table
    """
    __tablename__ = f'order_info_temp'
    __table_args__ = (Index('main', 'order_id', 'product_id', unique=True),
                      {'comment': 'Order Info',
                       'prefixes': ['TEMPORARY']})


class ExampleOrderTable(object):
    old_data = {'order_id': ['A0001', 'A0002', 'A0002'],
                'product_id': ['PD100', 'PD200', 'PD201'],
                'qty': [10, 20, 22],
                'refund_qty': [0, 0, 0]}
    new_data = {'order_id': ['A0001', 'A0002', 'A0002', 'A0003'],
                'product_id': ['PD100', 'PD200', 'PD201', 'PD300'],
                'qty': [10, 20, 22, 30],
                'refund_qty': [0, 0, 2, 0]}
    old_df = pd.DataFrame(old_data)
    new_df = pd.DataFrame(new_data)


if __name__ == '__main__':
    engine = Connector(schema='dev').get_engine()
    Base.metadata.create_all(bind=engine, tables=[Order.__table__])
    # also we can establish table like this below
    # Order.__table__.create(bind=engine)
