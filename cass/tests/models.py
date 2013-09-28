# models.py holds test models

from cass.fields import *
from cass.models.base import Model
from cass.models.base import ColumnFamilyTypes


class User(Model):
    """
    create table users (
      key text primary key,
      email text,
      fn text,
      ln text,
      loc text,
      dj timestamp
    );
    """

    class Meta:
        family = 'users'

    key = StringField(id_field=True)
    email = EmailField()
    first_name = StringField(field_name='fn')
    last_name = StringField(field_name='ln')
    location = StringField(field_name='loc')
    date_joined = DateTimeField(field_name='dj')


class Stream(Model):
    """
    create table streams (
      key uuid primary key,
      title text,
      des text,
      owner text,
      cat text
    );
    """
    class Meta:
        family = 'streams'

    key = UUIDField(id_field=True)
    title = StringField()
    description = StringField(field_name='des')
    owner = StringField()
    category = StringField(field_name='cat')


class UserStreams(Model):
    """
    secondary index, keeps record of streams for a user
    """
    class Meta:
        family = 'user_streams'
        # this model is persisted into a wide cf
        # aka dynamic cf
        family_type = ColumnFamilyTypes.WIDE

        # column_key must always be a UUID
        column_key = UUIDField

    key = StringField(id_field=True)