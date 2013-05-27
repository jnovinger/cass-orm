from django.test import TestCase
from django.conf import settings
from django.test.utils import override_settings
from cass.models.db import Cassandra
from pycassa import ConnectionPool, ColumnFamily


#@override_settings(CASSANDRA_DB=settings.CASSANDRA_TEST_DB)
class CassTestCase(TestCase):
    pass

    def setUp(self):
        # Use Test Database
        Cassandra.Instance().db = ConnectionPool(
            settings.CASSANDRA_TEST_DB,
            settings.CASSANDRA_HOSTS
        )
        # Truncate known tables
        Cassandra.Instance().cf('users').truncate()
        Cassandra.Instance().cf('user_streams').truncate()

        super(CassTestCase, self).setUp()