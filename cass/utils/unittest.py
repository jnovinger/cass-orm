from django.test import TestCase
from django.conf import settings
from cass.models.db import Cassandra


class CassTestCase(TestCase):

    def setUp(self):

        if hasattr(settings, 'TRUNCATE_CFS'):
            for cf in settings.TRUNCATE_CFS:
                Cassandra.Instance().cf(cf).truncate()

        super(CassTestCase, self).setUp()