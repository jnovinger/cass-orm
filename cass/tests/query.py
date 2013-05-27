from cass.utils.unittest import CassTestCase
from cass.tests.models import *
from cass.query import Cassandra

import datetime
import uuid

class TestSimpleModel(CassTestCase):
    """
    Tests models work with normal column families (schema defined)
    """

    def _create_user(self):
        user = User()
        user.key = 'lu'
        user.email = 'lu@test.com'
        user.first_name = 'lusana'
        user.last_name = 'ali'
        user.location = 'Australia'
        user.date_joined = datetime.datetime.utcnow()
        user.save()
        return user

    def test_insert(self):
        user = self._create_user()
        user_db = User.objects.get('lu')
        self.assertEqual(user.dict(), user_db.dict())

    def test_update(self):
        user = self._create_user()

        user_db = User.objects.get('lu')
        user_db.location = 'New Zealand'
        user_db.save()

        user_post_save = user.objects.get('lu')
        self.assertEqual(user_post_save.location, user_db.location)

    def test_remove(self):
        user = self._create_user()
        user.objects.remove('lu')

        # returns None when not found
        user_post_remove = user.objects.get('lu')
        self.assertIsNone(user_post_remove)


class TestWideModel(CassTestCase):
    """
    Tests wide column families (schema less/dynamic)
    """

    def test_insert(self):

        now = uuid.uuid1()
        index = UserStreams()
        index.key = 'lu'
        index.column_key = now
        index.save()

        then = uuid.uuid1()
        index = UserStreams()
        index.key = 'lu'
        index.column_key = then
        index.save()

        index_db = UserStreams.objects.get('lu')

        self.assertEqual(index_db[0].key, now)
        self.assertEqual(index_db[1].key, then)