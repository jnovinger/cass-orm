from dictshield.fields.base import BaseField
from django.utils import six
from cass.query import Query
import datetime
import uuid

from dictshield.base import ShieldDocException, ShieldException


class ColumnFamilyTypes(object):
    """
    Different column family types.
    WIDE: dynamic column family
    COUNTER: counter column family
    """
    WIDE = 'wide'
    COUNTER = 'counter'


class ModelMetaClass(type):
    """
    Meta class for all models
    """
    def __new__(cls, name, bases, attrs):

        super_new = super(ModelMetaClass, cls).__new__

        # Ignore Model itself
        if name == 'Model' or name == 'NewBase':
            return super_new(cls, name, bases, attrs)

        # Create the class.
        module = attrs.pop('__module__')
        new_class = super_new(cls, name, bases, {'__module__': module})
        attr_meta = attrs.pop('Meta', None)

        # Get Meta inner class
        if not attr_meta:
            raise AttributeError(
                "Meta class not found. Please see docs on Model usage"
            )

        query = attrs.pop('objects', None)

        # Get and set Query class
        klass = new_class
        query_object = Query(klass)

        if query:
            query_object = query
            query_object.model_class = klass
        setattr(new_class, 'objects', query_object)
        doc_fields = {}

        # set fields
        for attr_name, attr_value in attrs.items():
            has_class = hasattr(attr_value, "__class__")
            if has_class and issubclass(attr_value.__class__, BaseField):
                attr_value.field_name = attr_name
                if not attr_value.uniq_field:
                    attr_value.uniq_field = attr_name

                # get key field
                if attr_value.id_field:
                    setattr(new_class, '_key_field', attr_name)

                doc_fields[attr_name] = attr_value

            # set functions
            setattr(new_class, attr_name, attr_value)

        setattr(new_class, '_fields', doc_fields)
        setattr(new_class, '_meta', attr_meta)

        return new_class


class Model(six.with_metaclass(ModelMetaClass)):

    #__metaclass__ = ModelMetaClass

    def __init__(self):
        self._data = {}

    def to_python(self):
        """
        Returns a Python dictionary representing the Document's
        meta-structure and values.
        """
        fun = lambda f, v: f.for_python(v)
        data = self._to_fields(fun)
        return data

    def _to_fields(self, field_converter):
        """
        Returns a Python dictionary representing the Document's
        meta-structure and values.
        """
        data = {}

        # First map the subclasses of BaseField
        for field_name, field in self._fields.items():
            value = getattr(self, field_name, None)
            if value is not None:
                if field.id_field:
                    data[field.field_name] = field_converter(field, value)
                else:
                    data[field.uniq_field] = field_converter(field, value)

        return data

    def set_dict(self, dict_obj):
        self.dict_obj = dict_obj
        self.set_attr(dict_obj)

    def set_attr(self, dict_obj):
        for key, value in dict_obj.items():
            if isinstance(value, dict):
                self.set_attr(value)

            if hasattr(self, key):
                setattr(self, key, value)

    def save_wide(self, key, ttl=None):
        """
        Insert/updates a column into dynamic column family
        """
        if not self.column_key:
            raise ValueError("Wide table requires a column key")

        try:
            if isinstance(self.column_key, basestring):
                self.column_key = uuid.UUID(self.column_key)
        except ValueError:
            print '-'

        self.objects.insert(str(key), {
            self.column_key: str(self.to_python())
        }, ttl=ttl)

    def save(self, cols=None, ttl=None):
        """
        Insert/updates a row
        """
        try:
            key = getattr(self, self._key_field)
        except AttributeError:
            raise AttributeError("Key not set")


        if hasattr(self._meta, 'family_type') and \
                        self._meta.family_type == ColumnFamilyTypes.WIDE:
            return self.save_wide(key, ttl=ttl)

        row = self.to_python()

        if self._key_field in row:
            del row[self._key_field]
        self.objects.insert(key, row, ttl=ttl)

    def dict(self):
        """
        Use this to serialise the object back into a dict object
        """
        data = {}

        if hasattr(self, 'key'):
            data['key'] = str(self.key)

        if hasattr(self, 'column_key'):
            data['column_key'] = str(self.column_key)

        for key, value in self._data.items():
            if isinstance(value, uuid.UUID):
                value = str(value)
            elif isinstance(value, datetime.datetime):
                value = value.strftime('%m/%d/%Y')

            if value is not None:
                data[key] = value
        return data

    # @property
    # def column_key(self):
    #     return self._column_key
    #
    # @column_key.setter
    # def column_key(self, column_name):
    #     self._column_key = column_name

    def validate(self, validate_all=False):
        """
        Taken from dictshield
        Ensure that all fields' values are valid and that
        required fields are present.
        Throws a ShieldDocException if Document is invalid
        """
        # Get a list of tuples of field names and their current values
        fields = [(field, getattr(self, name))
                  for name, field in self._fields.items()]

        # Ensure that each field is matched to a valid value
        errs = []
        for field, value in fields:
            err = None
            # treat empty strings as nonexistent
            if value is not None and value != '':
                try:
                    field._validate(value)
                except ShieldException, e:
                    err = e
                except (ValueError, AttributeError, AssertionError):
                    err = ShieldException('Invalid value',
                                          field.field_name,
                                          value)
            elif field.required:
                err = ShieldException('Required field missing',
                                      field.field_name,
                                      value)
                # If validate_all, save errors to a list
            # Otherwise, throw the first error
            if err:
                errs.append(err)
            if err and not validate_all:
                # NB: raising a ShieldDocException in this case would be more
                # consistent, but existing code might expect ShieldException
                raise err

        if errs:
            raise ShieldDocException(self._class_name, errs)
        return True

