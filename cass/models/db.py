from pycassa import ConnectionPool, ColumnFamily
from pycassa.cassandra.ttypes import NotFoundException
from pycassa.columnfamilymap import ColumnFamilyMap
import logging, sys
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG, format="%(filename)s - %(funcName)s at %(lineno)d %(message)s")
from contextlib import contextmanager
from django.conf import settings


class Singleton:
    def __init__(self, decorated):
        self._decorated = decorated

    def Instance(self):
        try:
            return self._instance
        except AttributeError:
            self._instance = self._decorated()
            return self._instance

    def __call__(self):
        raise TypeError('Singletons must be accessed through `Instance()`.')

    def __instancecheck__(self, inst):
        return isinstance(inst, self._decorated)


class ClientPool(list):
    @contextmanager
    def reserve(self):
        mc = self.pop()
        try:
            yield mc
        finally:
            self.append(mc)


@Singleton
class Cassandra:
    def __init__(self):
        self.db = ConnectionPool(
            settings.CASSANDRA_DB,
            settings.CASSANDRA_HOSTS
        )
    def db(self):
        return self.db

    def cf(self, colfam):
        """
        Shorthand for column family
        """
        return ColumnFamily(self.db, colfam)

    def get(self, colfam, key, columns=None, column_start="", column_finish="",
            column_reversed=False, column_count=100, include_timestamp=False,
            super_column=None, read_consistency_level=None):

        cf = ColumnFamily(self.db, colfam)
        try:

            return cf.get(key, columns, column_start, column_finish,
                          column_reversed, column_count, include_timestamp,
                          super_column, read_consistency_level)
        except NotFoundException:
            return None


    def multiget(self, colfam, key, reversed=False, columns=None):
        cf = ColumnFamily(self.db, colfam)
        if columns is not None:
            return cf.multiget(key, columns)
        else:
            return cf.multiget(key)

    def insert(self, colfam, key, data):
        cf = ColumnFamily(self.db, colfam)
        return cf.insert(key, data)

    def get_count(self, colfam, key):
        cf = ColumnFamily(self.db, colfam)
        return cf.get_count(key)

    def remove(self, colfam, key, columns=None):

        cf = ColumnFamily(self.db, colfam)
        if columns is not None:
            return cf.remove(key, columns)
        else:
            return cf.remove(key)
