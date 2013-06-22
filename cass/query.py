import uuid
from pycassa import NotFoundException
from thrift.Thrift import TApplicationException
from models.db import Cassandra
from fields import *
import datetime

class Query(object):

    def __init__(self, model_class=None):
        # set the model class
        self.model_class = model_class
        self.cass = Cassandra.Instance()

    def insert(self, key, row):
        """
        Insert a new row. Idempotent, can be used for update as well
        """
        cf = self.cass.cf(self.model_class._meta.family)
        cf.insert(key, row)

    @property
    def _columnfamily(self):
        """
        Get the column family for the model
        """
        return Cassandra.Instance().cf(self.cf)

    def exists(self, key=None):
        """
        Checks whether the key exists.
        """

        return self.get(key, cols=[self.cls.Meta.key]) is not None


    def get_data(self, key=None, columns=None, column_start="", column_finish="",
                 column_reversed=False, column_count=100):

        cf = self.cass.cf(self.model_class._meta.family)

        try:

            data = cf.get(key,
                          columns=columns,
                          column_start=column_start,
                          column_finish=column_finish,
                          column_reversed=column_reversed,
                          column_count=column_count)
            return data
        except NotFoundException:
            return None

    def get_wide(self, key=None, columns=None, column_start="", column_finish="",
                 column_reversed=False, column_count=100):
        # wide column family means mapping multiple columns to their own object
        columns = self.check_columns_uuid(columns)

        all_data = self.get_data(
            key,
            columns,
            column_start,
            column_finish,
            column_reversed,
            column_count)

        models = []

        if all_data:

            models = [

                self.set_wide_fields(key, col_key, eval(data))
                for col_key, data in all_data.items()
            ]

        return models

    def set_wide_fields(self, row_key, column_key, data):
        """
        Create model instance from key, dict object returned by pycassa
        """
        model = self.model_class()

        # set all fields to None
        for field_name in model._fields.keys():
            setattr(model, field_name, None)

        # set key
        setattr(model, 'key', row_key)
        setattr(model, 'column_key', str(column_key))
        setattr(model, model._key_field, row_key)

        # set fields
        for key, value in data.items():
            for field_name, field_obj in model._fields.items():
                if field_obj.uniq_field == key:
                    setattr(model, field_name, value)
                    break

        return model

    def set_fields(self, key, data):
        """
        Create model instance from key, dict object returned by pycassa
        """
        model = self.model_class()

        # set all fields to None
        for field_name in model._fields.keys():
            setattr(model, field_name, None)

        # set key
        setattr(model, 'key', key)
        setattr(model, model._key_field, key)

        # set fields
        for key, value in data.items():
            for field_name, field_obj in model._fields.items():
                if field_obj.uniq_field == key:
                    setattr(model, field_name, value)
                    break

        return model

    def get(self, key=None, columns=None, column_start="", column_finish="",
            column_reversed=False, column_count=100):

        # Check if wide row.
        if hasattr(self.model_class._meta, 'family_type'):
            if self.model_class._meta.family_type == 'wide':
                return self.get_wide(key, columns, column_start, column_finish, column_reversed, column_count)

        if key is None or (hasattr(key, 'value') and key.value is None):
            return None

        cf = self.cass.cf(self.model_class._meta.family)
        try:
            data = cf.get(key,
                          columns=columns,
                          column_start=column_start,
                          column_finish=column_finish,
                          column_reversed=column_reversed,
                          column_count=column_count)
            return self.set_fields(key, data)

        except NotFoundException, TApplicationException:
            return None

    def multiget(self, keys, columns=None, column_start="", column_finish="",
                 column_reversed=False, column_count=100, include_timestamp=False,
                 super_column=None, read_consistency_level=None, buffer_size=None):

        cf = self.cass.cf(self.model_class._meta.family)
        all_data = cf.multiget(keys, columns=columns,
                               column_start=column_start, column_finish=column_finish)

        models = []
        for key, data in all_data.items():
            model = self.set_fields(key, data)
            models.append(model)
        return models

    def get_range(self, start="", finish="", columns=None, column_start="",
                  column_finish="", column_reversed=False, column_count=100,
                  row_count=None, include_timestamp=False,
                  super_column=None, read_consistency_level=None,
                  buffer_size=None, filter_empty=True):

        cf = self.cass.cf(self.model_class._meta.family)

        all_data = cf.get_range(row_count=row_count, columns=columns)
        models = []
        for key, data in all_data:
            model = self.set_fields(key, data)
            models.append(model)
        return models


    def check_columns_uuid(self, columns):

        if columns is None:
            return columns

        meta = self.model_class._meta

        # check if we need columns to be uuids
        if isinstance(meta.column_key, UUIDField):
            if not isinstance(columns, list):
                columns = [columns]

            col_uuids = []

            for col in columns:
                if not isinstance(col, uuid.UUID):
                    col_uuids.append(uuid.UUID(col))

            columns = col_uuids

        return columns

    def remove(self, key=None, columns=None):
        """
        Removes the entire row or in the case of a wide column
        only remove certain columns
        """
        columns = self.check_columns_uuid(columns)
        cf = Cassandra.Instance().cf(self.model_class._meta.family)
        cf.remove(key, columns)