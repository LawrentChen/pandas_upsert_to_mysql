# !/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Customize MySQL upsert method

Note:
    - You can only have the auto incremented field as the only column in primary key
    - The method here **will not** automatically create target table
"""
from contextlib import contextmanager

from sqlalchemy.dialects.mysql import insert
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker


class Upsert(object):
    """
    Customize upsert to MySQL

    :param sqlalchemy.engine.base.Engine engine: SQLAlchemy engine
    """

    def __init__(self, engine):
        self.engine = engine

    @contextmanager
    def _session_scope(self, scoped=True):
        """
        Provide a transactional scope around a series of operations.

        :param bool scoped: default True, not sure whether this can make thread safe or not

        Usage:
        >>> with self._session_scope() as session:
        >>>     # Some sql operations
        >>>     pass
        """
        if not scoped:
            session = sessionmaker(bind=self.engine)()
        else:
            session = scoped_session(sessionmaker(bind=self.engine))
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    @staticmethod
    def _my_insert(target_table, temp_table, session, if_record_exists):
        """
        Insert or update data from temp_table to target_table

        :param SQLAlchemy.ext.declarative.api.DeclarativeMeta target_table: target table
        :param SQLAlchemy.ext.declarative.api.DeclarativeMeta temp_table: corresponding temporary table
        :param session: SQLAlchemy session
        :param str if_record_exists: {'update', 'ignore'}

            the strategy to deal with existed records(identified by primary key or unique constraint)

            - 'update': INSERT INTO target_table (column_list) SELECT column_list FROM temp_table
                        ON DUPLICATE KEY UPDATE col=VALUES(col)
            - 'ignore': INSERT IGNORE INTO target_table (column_list) SELECT column_list FROM temp_table

        :return: None
        """
        temp_inspect = inspect(temp_table)

        # convey type to columns list
        column_list = [c for c in temp_inspect.columns]
        pk_list = [k for k in temp_inspect.primary_key]

        # get rid of the table info
        bare_column_list = [c.key for c in temp_inspect.columns]
        bare_pk_list = [k.name for k in temp_inspect.primary_key]

        # get the column names from temp_table except primary key (which must be an auto incremented field here)
        for k in bare_pk_list:
            bare_column_list.remove(k)

        for k in pk_list:
            column_list.remove(k)

        if if_record_exists == 'update':
            stmt = insert(target_table).from_select(bare_column_list, session.query(*column_list))
            update_dict = {column: stmt.inserted[f'{column}'] for column in bare_column_list}
            stmt = stmt.on_duplicate_key_update(update_dict)
        elif if_record_exists == 'ignore':
            stmt = insert(target_table).from_select(bare_column_list, session.query(*column_list))
            stmt = stmt.prefix_with('IGNORE')
        else:
            raise ValueError('if_record_exists 参数只接受 update 或 ignore')
        session.execute(stmt)

    def to_mysql(self, df, target_table, temp_table, if_record_exists):
        """
        Insert or update a DataFrame to target_table through a temp_table

        :param DataFrame df: target DataFrame
        :param SQLAlchemy.ext.declarative.api.DeclarativeMeta target_table: target table's ORM class
        :param SQLAlchemy.ext.declarative.api.DeclarativeMeta temp_table: temp table's ORM class
        :param str if_record_exists: {'update', 'ignore'}

            upsert strategy

            - 'update': update existing records

                        which equals to::

                            INSERT INTO target_table (column_list) SELECT column_list FROM temp_table ON DUPLICATE KEY
                            UPDATE col=VALUES(col)

                        Mind that 'INSERT INTO ... ON DUPLICATE KEY UPDATE' is different from 'REPLACE INTO...',
                        it won't update those fields in database but not show up in DataFrame.
                        But our method here is still unable to achieve that.

                        If a record is not been update since it is completely identical to the one in DataFrame,
                        its update_time field won't be changed.

            - 'ignore': ignore existing records

                        which equals to::

                            INSERT IGNORE INTO target_table (column_list) SELECT column_list FROM temp_table

        :return: None
        """
        if if_record_exists not in ('update', 'ignore'):
            raise ValueError('if_record_exists must be "update" or "ignore"')

        # temporary table will be deleted automatically when the session is disconnected
        # using context manager here
        with self._session_scope() as session:
            temp_table.__table__.create(bind=session.connection())

            try:
                df.to_sql(temp_table.__tablename__, con=session.connection(),
                          if_exists='append', index=False, chunksize=1000)
                # when inserting temp_table,if the fields in DataFrame
                #   - more than those defined in ORM: raise error
                #   - less than those defined in ORM:upsert the fields in DataFrame only
                #   - different from those defined in ORM: raise error
            except Exception as e:
                raise e
            else:
                self._my_insert(target_table=target_table, temp_table=temp_table, session=session,
                                if_record_exists=if_record_exists)


if __name__ == '__main__':
    pass
