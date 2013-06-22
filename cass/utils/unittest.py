from django.test import TestCase
from django.conf import settings
from django.test.utils import override_settings
from cass.models.db import Cassandra
from pycassa import ConnectionPool, ColumnFamily


class CassTestCase(TestCase):

    def setUp(self):

        if hasattr(settings, 'TRUNCATE_CFS'):
            for cf in settings.TRUNCATE_CFS:
                Cassandra.Instance().cf(cf).truncate()

        Cassandra.Instance().cf('users').truncate()
        Cassandra.Instance().cf('user_emails').truncate()
        Cassandra.Instance().cf('user_friends').truncate()

        super(CassTestCase, self).setUp()