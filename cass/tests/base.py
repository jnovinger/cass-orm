from unittest import TestCase
from cass.models.base import Model
from cass.fields import *
import datetime

class TestModel(TestCase):
    """
    Tests functions in models.base.py,
    does not test any actual database queries
    """

    def test_to_python(self):
        """
        Test to_python will create the correct dict
        """
        class TestUserModel(Model):

            # need Meta and family so we don't get errors
            class Meta:
                family = "test_family"

            username = StringField(id_field=True)
            first_name = StringField(field_name='fn')
            last_name = StringField()
            age = IntField()
            date_joined = DateTimeField(field_name='dj')

        now = datetime.datetime.utcnow()
        user = TestUserModel()
        user.username = "harry"
        user.first_name = "Harry"
        user.last_name = "Potter"
        user.age = 16
        user.date_joined = now

        expected_dict = {
            'username': u'harry',
            'age': 16,
            'last_name': u'Potter',
            # field_name provided
            'fn': u'Harry',
            'dj': now
        }
        self.assertDictEqual(user.to_python(), expected_dict)
