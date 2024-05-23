
from datetime import datetime, date, time
from decimal import Decimal
import logging
from operator import attrgetter
import random
from uuid import UUID as HsnUUID

import elixir
from sqlalchemy.dialects.postgresql import INET, UUID
import sqlalchemy.types

from backend.core import _class_repr, \
                         _field_repr, \
                         generate_id, \
                         retry_operational_error
from backend.environments.core import Entity
from util.when import from_hsndate, from_hsntime, from_timestorage, \
                      to_hsndate, to_hsntime, to_timestorage


logger = logging.getLogger("hsn.backend")

"""A Note about Cassandra types:

This is a basic guideline for what backend uses for types but it is not
necessarily spec.  Each Field in backend.schema declares a class
variable 'cassandra_type'.  If you want the cassandra_type for an
Entity's attribute use this:
entity_class.get_attribute('myattribute').cassandra_type
If we need a mapping from python-type to cassandra-type that is not
supported with an existing backend.schema Field it is likely we will
declare a new Field type with the desired python-type-to-cassandra-type
mapping.

python type -- cassandra type

bool -- boolean
date -- int
datetime -- bigint
Decimal -- decimal,  Variable-precision decimal
float -- double,  32-bit IEEE-754 floating point
int -- int,  32-bit signed integer
long -- bigint,  64-bit signed long (note Cassandra has varint, arb prec)
str -- text,  UTF-8 (was blob: arbitrary bytes, expressed as hexadecimal)
time -- bigint
unicode -- text,  UTF-8 encoded string
UUID -- uuid,  type 4 UUID (type 1's time properties are ignored with this type)
UUID -- timeuuid,  type 1 UUID

"""

class Persistent(object):
    """Baseclass of a Backend object declaration.

    Duck-type equivalent of an Entity as implemented in Sql Alchemy.

    (note: Persistent is effectively an Elixir.Entity subclass.)
    
    """

    __metaclass__ = elixir.EntityMeta
    saves_to_cassandra = True
    saves_to_postgresql = True
    # Declare Cassandra composite key indexes.
    # indexes = {
    #     'c1': ('uuid', 'str_field', 'enum_field', 'inc_field', 'int_field'),
    #     'c2': ('str_field', 'inc2_field', 'uuid', 'datetime_field')
    # }
    indexes = {}
    @property
    def is_entity(self):
        return False

    @property
    def is_persistent(self):
        return True

    # (Copied from Entity.py)
    @classmethod
    @retry_operational_error()
    def get_by(cls, *args, **kwargs):
        return cls.query.filter_by(*args, **kwargs).first()

    def delete(self, *args, **kwargs):
        logger.error('Delete needs to be implemented on a per-object basis')
        raise NotImplementedError

    @property
    def key(self):
        return getattr(self, self.entity_class.key_attribute_name)

    @property
    def has_temporary_key(self):
        key = getattr(self, self.entity_class.key_attribute_name)
        return key is None

    @property
    def write_only(self):
        return False # No such thing in Persistent land.

    @classmethod
    def create(cls, key=None):
        if key is not None:
            # TODO: Is environment persistent a singleton?
            from backend.environments.persistent import EnvironmentPersistent
            env = EnvironmentPersistent()
            obj = env.get(cls.entity_class, key)
            assert obj is None
            return env.fetch(cls.entity_class, key, has_temporary_key=False)
        else:
            obj = cls.persistent_class()
            if obj.entity_class.key_attribute_name == 'uuid':
                obj.uuid = generate_id()
            else:
                raise NotImplementedError
            return obj

    def get_peer(self, entity):
        return self.environment.get_peer(entity)
#        return obj.persistent_class.get_by(**{obj.key_attribute_name:obj.key})

    def fetch_peer(self, entity):
        return self.environment.fetch_peer(entity)

    @property
    def environment(self):
        from backend.environments.persistent import MainPersistent
        return MainPersistent() # ? should be an instance singleton?
        # note that this does not support different sources. If we ever need
        # that it goes here.

    @property
    def attributes(self):
        return self.entity_class.attributes

    @property
    def attributes_order(self):
        return self.entity_class.attributes_order

    @classmethod
    def fields(cls):
        builders = cls._descriptor.builders

        if Persistent not in cls.__bases__:
            #Base DB Class
            for each in cls.__bases__:
                #Check if base_class is DB class, or a Mix-In
                if issubclass(each, Persistent):
                    builders += each.fields()

        return builders

    def get_persistent(self):
        return self

    def __eq__(self, other):
        #Have to check if other has a __hash__ function.
        # if it doesn't, that implies it's not an entity or a persistent
        return other.__hash__ and (self.__hash__() == other.__hash__())

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        h = str('__entity__%s_%s' % (
            self.entity_class.__name__, self.key)).__hash__()
        # TODO: This assumes that all type names are unique, so nothing like
        # model.obj.Foo and model.person.Foo.
        return h

    def __repr__(self):
        return u'%s(%s)' % (self.__class__.__name__, getattr(self, self.entity_class.key_attribute_name))


class Attribute(object):
    """Base class for any Backend-aware attribute.

    Holds a convenient representation of Field metadata.

    A Persistent declaration is this system's schema declaration. During
    system setup each Entity subclass creates a set of Attributes, one for
    each Field in the Persistent declaration.

    (Fields are not pickleable, but attributes are, and can be used as
    variables in Deltas and WriteOnlies. Note that when kept as class variables
    they don't get pickled. This is why they are initialized on system start.)

    """
    # Note: This object is only created at setup time. It is made from a Field.

    # TODO: Put some type-checking in here. An Attribute should know what type
    # is a legal value and assert.

    def __init__(self,
                 name,
                 value_class, # Type of value held by this attribute.
                 entity_class, # Entity Subclass holding this attribute.
                 field_instance,
                 related_attribute=None,    # _SchemaObj subclass this relationship connects to. None if n/a.
                 enum=None,
                 is_set_like=False,
                 autoload=False,
                 default=None,
                 is_sql_primary_key=False,
                 sql_table_name=None,
                 cassandra_type=None):
        self.name = name
        self.value_class = value_class
        self.entity_class = entity_class
        self.field_instance = field_instance
        self.related_attribute = related_attribute
        self.enum = enum
        self.is_set_like = is_set_like
        self.autoload = autoload
        self.default = default
        self.is_sql_primary_key = is_sql_primary_key
        self.sql_table_name=sql_table_name
        self.cassandra_type=cassandra_type
        # Schema-based cassandra composite key indexes this is a key in.
        self.indexes = set()  # index names (str), see persistent_class.indexes

    def cassandra_encode(self, value):
        """Encode Attribute's value for passing to a util.cassandra func."""
        return self.field_instance.cassandra_encode(value)

    def cassandra_decode(self, value):
        return self.field_instance.cassandra_decode(value)

    def __hash__(self):
        return ('attribute_%s_%s' % (self.entity_class.__name__,
                                     self.name)).__hash__()

    def __repr__(self):
        return u'%s(%s.%s)' % (self.__class__.__name__,
                               self.entity_class.__name__,
                               self.name)


# This is a helper for Relationship Fields (only) to provide a make_attribute
class MakeAttribute(object):

    def get_value_class(self):
        # TODO
        return None
        from backend.core import get_class
        return get_class(self._related_persistent_class).entity_class

    def make_attribute(self, parent_class):
        return Field._make_attribute(self, parent_class)

class OneToOne(Attribute):
    def cassandra_encode(self, value):
        raise ValueError, 'OneToOne does not encode to Cassandra'

    def cassandra_decode(self, value):
        raise ValueError, 'OneToOne does not decode from Cassandra '

# Other side of one-to-one relationship in Elixir is a FakeManyToOne.
class FakeManyToOne(Attribute):
    def cassandra_encode(self, value):
        if value is None:
            return None
        else:
            # Keys are either UUIDs or strings which encode as they are.
            return value.key

    def cassandra_decode(self, value):
        if not value:  # could be '' due to legacy reasons, should be None.
            return None
        else:
            return self.related_attribute.entity_class(value)

class ManyToOne(Attribute):
    def cassandra_encode(self, value):
        if value is None:
            return None
        else:
            # Keys are either UUIDs or strings which encode as they are.
            return value.key

    def cassandra_decode(self, value):
        if not value:  # could be '' due to legacy reasons, should be None.
            return None
        else:
            return self.related_attribute.entity_class(value)

class OneToMany(Attribute):
    def cassandra_encode(self, value):
        raise ValueError, 'OneToMany does not encode to Cassandra'

    def cassandra_decode(self, value):
        raise ValueError, 'OneToMany does not decode from Cassandra '

# Not Used.
class ManyToMany(Attribute):
    pass


OneToOne.reciprocal_attribute = FakeManyToOne
FakeManyToOne.reciprocal_attribute = OneToOne
ManyToOne.reciprocal_attribute = OneToMany
OneToMany.reciprocal_attribute = ManyToOne
ManyToMany.reciprocal_attribute = ManyToMany


class Incrementer(Attribute):
    pass


class EnumValue(object):
    """A single value in an Enum's domain. It has a name and a number."""

    def __init__(self, set_name, number, name, enumset=None):
        assert(type(22) == type(number))
        self.number = number
        assert(type(u'') == type(name))
        self.name = name
        self.set_name = set_name
        self._enumset = enumset

    @property
    def as_pair(self):
        return (self.number, self.name)

    def __repr__(self):
        return 'EnumValue(%s, %s, %s)' % (self.set_name, self.number, self.name)

    def __eq__(self, other):
        if (type(other) == EnumValue):
            same_number = (self.number == other.number)
            same_name = (self.name == other.name)
            same_set = (self.set_name == other.set_name)
            return same_number and same_name and same_set
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        hash_string = '%s_%s' % (self.set_name, self.number)
        return hash_string.__hash__()


class Enum(object):
    """Maps a set of keys to a numbered representation in Backend storage."""

    def __init__(self, enum_name, names, not_specified_name=None):
        self._enum_name = enum_name
        # The pairs as (number, name) in order.
        # (this is useful for seeding UI elements.)
        self._pairs = []
        self._in_order = []
        # fast-lookup maps
        self._by_name = {}
        self._by_number = {}

        # Make pairs out of the given names, and store them in this set.
        i = 0
        for name in names:
            assert(type(u'') == type(name))
            new_pair = EnumValue(enum_name, i, name, enumset=self)
            i += 1
            self._pairs.append((new_pair.number, new_pair.name))
            self._in_order.append(new_pair)
            self._by_name[new_pair.name] = new_pair
            self._by_number[new_pair.number] = new_pair

        if not_specified_name:
            not_specified_pair = EnumValue(enum_name, -1, not_specified_name, enumset=self)
            self._pairs.append((not_specified_pair.number, not_specified_pair.name))
            self._in_order.append(not_specified_pair)
            self._by_name[not_specified_pair.name] = not_specified_pair
            self._by_number[not_specified_pair.number] = not_specified_pair

    def __getitem__(self, key_or_number):
        """Looks up by name or number using [] operator."""
        if isinstance(key_or_number, unicode):
            return self._by_name[key_or_number]
        elif isinstance(key_or_number, int):
            return self._by_number[key_or_number]
        else:
            text = "__getitem__ does not support an index of type '%s'"
            raise TypeError(text % type(key_or_number))

    def random(self):
        return random.choice(self._in_order)

    @property
    def by_name(self):
#        logger.warn("Deprecated. Use []")
        return self._by_name

    @property
    def by_number(self):
#        logger.warn("Deprecated. Use []")
        return self._by_number

    @property
    def names(self):
        return self._by_name.keys()

    @property
    def pairs(self):
        """Return this Enum as a set of (number, name) tuples."""
        return self._pairs

    @property
    def in_order(self):
        """Return the contents of this Enum in order."""
        return self._in_order

    @property
    def sorted(self):
        """Returns the contents of this Enum sorted by name"""
        sorted_pairs = sorted(self._in_order, key=attrgetter('name'))
        return sorted_pairs

    @property
    def pairs_sorted(self):
        """Returns pairs for this Enum sorted by name"""
        sorted_pairs = sorted(self.pairs, key=lambda p:p[1])
        return sorted_pairs

    def by_columns(self, columns=3, sort='name', put_last=[]):
        """Return the contents of this Enum sorted by 'sort'
           and in a the given number of columns

           sort='name'   --> sort by name
           sort='number' --> sort by number

           removed=list of items (by name or number) to be removed

        """
        if sort=='name':
            sorted_list = self.pairs_sorted[:]  #makes a copy of list, so we don't accidentally change the original
        elif sort=='number':
            sorted_list = self.pairs[:]
        else:
            raise NotImplementedError

        #put last items (in the order they are presented
        for r in put_last:
            sorted_list.remove(self[r].as_pair)
            sorted_list.append(self[r].as_pair)

        length = len(sorted_list)
        rows = length/columns
        extra = length - (rows*columns)

        #we might have lost one row due to rounding
        if extra > 0:
            rows += 1

        by_cols = []
        for row in range(rows):
            for col in range(columns):
                indx=row+(rows*col)
                if indx<length:
                    if extra != 0 and row==rows-1 and col>=extra:
                        #last row is shortened if extra is set
                        pass
                    else:
                        by_cols.append(sorted_list[indx])

        return by_cols

    def __repr__(self):
        return "%s : %s" % (_class_repr(self), self[0].name)


Months = Enum(u'Months',
              [u'Month',
               u'January',
               u'February',
               u'March',
               u'April',
               u'May',
               u'June',
               u'July',
               u'August',
               u'September',
               u'October',
               u'November',
               u'December'])

               
class Field(elixir.Field, MakeAttribute):
    """A unit of cachable persistable storage."""
    attribute_class = Attribute
    value_class = None
    is_set_like = False
    enum = None

    def __init__(self, *args, **kwargs):
        autoload = False
        if 'autoload' in kwargs:
            autoload = kwargs['autoload']
            del kwargs['autoload']
        self.autoload = autoload
        if 'default' in kwargs:
            self.default = kwargs['default']
        else:
            self.default = None
        super(Field, self).__init__(*args, **kwargs)

    def get_value_class(self):
        return self.value_class

    # Note: This is the only constructor for Attributes that should be called.
    # Further Note: Unless the entities are not based on SQL Persistent
    # objects like QueryRelationship.
    @staticmethod # called from MakeAttribute
    def _make_attribute(field_instance, parent_class):
        """Create Attribute defined by the given Field and Entity subclass."""
        # This is a kludge until I can figure out how to read the defaults
        # from elixir fields.

        # TODO: This is a little messed, why not just call Attribute(field)

        # Check for illegal names.  This can lead to errors in CQL because
        # certain column names are disallowed.
        # Note: We could allow these names if were were to quote them anywhere
        # they appear in our CQL.
        if field_instance.name.lower() in DISALLOWED_FIELD_NAMES:
            # We have one grandfathered case here.
            if parent_class.__name__ in ['LoginToken', 'OAuthToken', 'SQLLoginToken'] and field_instance.name.lower() == 'token':
                pass
            elif parent_class.__name__ in ['KeyValue', 'Trait'] and field_instance.name.lower() == 'key':
                pass
            else:
                raise ValueError, "disallowed Field name %s.%s" % (parent_class.__name__, field_instance.name)
        if field_instance.name.lower().startswith('_'):
            raise ValueError, "Field name can't start with '_' %s.%s" % (parent_class.__name__, field_instance.name)

        # Create an Attribute with the information known.
        if isinstance(field_instance, (ManyToManyField, ManyToOneField,
                                       OneToManyField, OneToOneField,
                                       FakeManyToOneField, OneToNPManyField,
                                       ManyToNPOneUUIDField,
                                       FakeUUIDManyToNPOneField,
                                       OneToNPOneField)):
            is_sql_primary_key = False
        else:
            is_sql_primary_key = field_instance.primary_key
        sql_table_name = None
        if isinstance(field_instance, (ManyToOneField, FakeManyToOneField, EnumSetField)):
            sql_table_name = field_instance.entity.table.name
        if sql_table_name is None and hasattr(field_instance, 'column'):
            c = field_instance.column
            if hasattr(c, 'table'):
                t = c.table
                sql_table_name = t.name            
        attribute = field_instance.attribute_class(
            field_instance.name,
            field_instance.get_value_class(),
            parent_class,
            field_instance,
            enum=field_instance.enum,
            autoload=field_instance.autoload,
            is_set_like=field_instance.is_set_like,
            default=field_instance.default,
            is_sql_primary_key=is_sql_primary_key,
            sql_table_name=sql_table_name,
            cassandra_type=field_instance.cassandra_type)
        # see TODO above about Attribute(field)
        if isinstance(attribute, ManyToMany):
            raise NotImplementedError('ManyToMany is no longer supported')
            cols = list(field_instance.table.columns)
            assert len(cols) == 2
            assert len(cols[0].foreign_keys) == 1
            assert len(cols[1].foreign_keys) == 1
            attribute._column_data = {
                cols[0].name : list(cols[0].foreign_keys)[0].column.table.name,
                cols[1].name : list(cols[1].foreign_keys)[0].column.table.name
            }
        return attribute

    def cassandra_encode(self, value):
        return value

    def cassandra_decode(self, value):
        return value

    def __repr__(self):
        return _field_repr(self)


class UnicodeField(Field):
    value_class = unicode
    cassandra_type = 'text'

    def __init__(self, size=None, **kwargs):
        if size:
            super(UnicodeField, self).__init__(elixir.Unicode(size), **kwargs)
        else:
            super(UnicodeField, self).__init__(elixir.Unicode, **kwargs)


class ImagecodeField(UnicodeField):
    def __init__(self, unique=True, *args, **kwargs):
        super(ImagecodeField, self).__init__(200, unique=unique, default=None, *args, **kwargs)


class StringField(Field):
    value_class = str
    cassandra_type = 'text'

    def __init__(self, size=10000, **kwargs):
        super(StringField, self).__init__(elixir.String(size), **kwargs)

    def cassandra_decode(self, value):
        if value is None:
            return None
        else:
            return str(value)

# This is a helper for Relationship Fields to provide data to an Attribute.
class HasRelatedPersistentClass(object):
    @property
    def related_persistent_class(self):
#        rpc = self._related_persistent_class
#        if type('') == type(rpc):
#            self._related_persistent_class = get_class(rpc) # Total kludge
#        return self._related_persistent_class
        return self.target


class OneToManyField(elixir.OneToMany, HasRelatedPersistentClass, MakeAttribute):
    attribute_class = OneToMany
    is_set_like = True
    enum = None
    cassandra_type = None

    def __init__(self, fields, **kwargs):
        autoload = False
        if 'autoload' in kwargs:
            autoload = kwargs['autoload']
            del kwargs['autoload']
        self.autoload = autoload
        if 'default' in kwargs:
            self.default = kwargs['default']
        else:
            self.default = []
        self._related_persistent_class = fields
        super(OneToManyField, self).__init__(fields, **kwargs)

    def __repr__(self):
        return _field_repr(self)


class ManyToOneField(elixir.ManyToOne, HasRelatedPersistentClass, MakeAttribute):
    attribute_class = ManyToOne
    is_set_like = False
    enum = None
    cassandra_type = None

    def __init__(self, fields, **kwargs):
        autoload = False
        if 'autoload' in kwargs:
            autoload = kwargs['autoload']
            del kwargs['autoload']
        self.autoload = autoload
        if 'default' in kwargs:
            self.default = kwargs['default']
        else:
            self.default = None
        self._related_persistent_class = fields
        super(ManyToOneField, self).__init__(fields, **kwargs)

    def __repr__(self):
        return _field_repr(self)


class FakeManyToOneField(ManyToOneField):
    attribute_class = FakeManyToOne
    is_set_like = False


# Not Used.
class ManyToManyField(elixir.ManyToMany, HasRelatedPersistentClass, MakeAttribute):
    attribute_class = ManyToMany
    is_set_like = True
    enum = None
    cassandra_type = None

    def __init__(self, fields, **kwargs):
        autoload = False
        if 'autoload' in kwargs:
            autoload = kwargs['autoload']
            del kwargs['autoload']
        self.autoload = autoload
        if 'default' in kwargs:
            self.default = kwargs['default']
        else:
            self.default = []
        self._related_persistent_class = fields
        super(ManyToManyField, self).__init__(fields, **kwargs)

    def __repr__(self):
        return _field_repr(self)


class OneToOneField(elixir.OneToOne, HasRelatedPersistentClass, MakeAttribute):
    attribute_class = OneToOne
    is_set_like = False
    enum = None
    cassandra_type = None

    def __init__(self, related_persistent_class, **kwargs):
        autoload = False
        if 'autoload' in kwargs:
            autoload = kwargs['autoload']
            del kwargs['autoload']
        self.autoload = autoload
        if 'default' in kwargs:
            self.default = kwargs['default']
        else:
            self.default = None
        self._related_persistent_class = related_persistent_class
        super(OneToOneField, self).__init__(related_persistent_class, **kwargs)

    def __repr__(self):
        return _field_repr(self)


#---
#class NonElixirField(object):
#    pass

# These are the 'NP' versions of the relationship fields.  These are necessary
# for having relationships to objects that do not save to Postgresql.
class OneToNPManyField(MakeAttribute):
    """Relationship from one to a non-postgresql many."""
    attribute_class = OneToMany
    is_set_like = True
    enum = None
    cassandra_type = None

    def __init__(self, related_persistent_class_name, inverse='', *args, **kwargs):
        autoload = False
        if 'autoload' in kwargs:
            autoload = kwargs['autoload']
            del kwargs['autoload']
        self.autoload = autoload
        if 'default' in kwargs:
            self.default = kwargs['default']
        else:
            self.default = []
        self.related_persistent_class_name = related_persistent_class_name
        if inverse:
            self.inverse = inverse
#        self._related_persistent_class = fields
#        super(OneToManyField, self).__init__(fields, **kwargs)

    def __repr__(self):
        return _field_repr(self)


class ManyToNPOneUUIDField(Field, MakeAttribute):
    """Relationship from a many to a non-postgresql one.

    This assumes the related primary key is a UUID.  If we need something else
    we will need to make it its own Field class.

    """
    value_class = HsnUUID
    attribute_class = ManyToOne
    is_set_like = False
    enum = None
    cassandra_type = None

    def __init__(self, related_persistent_class_name, inverse='', *args, **kwargs):
        autoload = False
        if 'autoload' in kwargs:
            autoload = kwargs['autoload']
            del kwargs['autoload']
        self.autoload = autoload
        if 'default' in kwargs:
            self.default = kwargs['default']
        else:
            self.default = None
        if inverse:
            self.inverse = inverse
        self.related_persistent_class_name = related_persistent_class_name
        super(ManyToNPOneUUIDField, self).__init__(UUIDType, *args, **kwargs)

    def __repr__(self):
        return _field_repr(self)


class FakeUUIDManyToNPOneField(Field, MakeAttribute):
    """Relationship from a OneToOne to a non-postgresql one.

    This is the side of the relationship that holds the key.

    """
    value_class = HsnUUID
    attribute_class = FakeManyToOne
    is_set_like = False

    def __init__(self, related_persistent_class_name, inverse='', *args, **kwargs):
        autoload = False
        if 'autoload' in kwargs:
            autoload = kwargs['autoload']
            del kwargs['autoload']
        self.autoload = autoload
        if 'default' in kwargs:
            self.default = kwargs['default']
        else:
            self.default = None
        if inverse:
            self.inverse = inverse
        self.related_persistent_class_name = related_persistent_class_name
        super(FakeUUIDManyToNPOneField, self).__init__(UUIDType, *args, **kwargs)

    def __repr__(self):
        return _field_repr(self)


class OneToNPOneField(MakeAttribute):
    """Relationship from a one to a non-postgresql one.

    This is the side of the relationship that does not hold the key.

    """
    attribute_class = OneToOne
    is_set_like = False
    enum = None
    cassandra_type = None

    def __init__(self, related_persistent_class_name, inverse='', *args, **kwargs):
        autoload = False
        if 'autoload' in kwargs:
            autoload = kwargs['autoload']
            del kwargs['autoload']
        self.autoload = autoload
        if 'default' in kwargs:
            self.default = kwargs['default']
        else:
            self.default = None
        if inverse:
            self.inverse = inverse
        self.related_persistent_class_name = related_persistent_class_name

    def __repr__(self):
        return _field_repr(self)


class IntegerField(Field):
    value_class = int
    cassandra_type = 'int'

    def __init__(self, **kwargs):
        super(IntegerField, self).__init__(elixir.Integer, **kwargs)


class IncrementField(Field):
    attribute_class = Incrementer
    value_class = int
    cassandra_type = 'int'

    def __init__(self, **kwargs):
        super(IncrementField, self).__init__(elixir.Integer, **kwargs)


class LongField(Field):
    value_class = long
    cassandra_type = 'bigint'
    
    def __init__(self, **kwargs):
        super(LongField, self).__init__(elixir.BigInteger, **kwargs)


class FloatField(Field):
    value_class = float
    cassandra_type = 'double'

    def __init__(self, **kwargs):
        super(FloatField, self).__init__(elixir.Float, **kwargs)

class DecimalField(Field):
    value_class = Decimal
    cassandra_type = 'decimal'

    def __init__(self, precision=2, **kwargs):
        super(DecimalField, self).__init__(elixir.DECIMAL(precision), **kwargs)

class IPAddressField(Field):
    value_class = unicode
    cassandra_type = 'text'

    def __init__(self, *args, **kwargs):
        super(IPAddressField, self).__init__(IPAddrType, *args, **kwargs)

    def __repr__(self):
        return "IPAddr(%s, %s)" % (self.name, self.entity)


class UUIDField(Field):
    value_class = HsnUUID
    cassandra_type = 'uuid'

    def __init__(self, unique=True, index=True, *args, **kwargs):
        super(UUIDField, self).__init__(UUIDType, unique=unique, index=index, *args, **kwargs)

    def __repr__(self):
        return "UUID(%s, %s)" % (self.name, self.entity)


class TimeUUIDField(UUIDField):
    cassandra_type = 'timeuuid'


class EnumField(Field):
    """Field for holding Integer values that are mapped to an EnumValue."""
    value_class = EnumValue
    cassandra_type = 'int'

    def __init__(self, enum, **kwargs):
        self.enum = enum
        tipe = EnumType()
        tipe.enum = enum
        super(EnumField,self).__init__(tipe, **kwargs)

    def cassandra_encode(self, value):
        if value is None:
            return None
        else:
            return value.number

    def cassandra_decode(self, value):
        if value is None or value == '':
            return None
        else:
            return self.enum[int(value)]


class EnumSetField(OneToManyField):
    """Holds a set of integer values that are mapped to EnumValues."""
    value_class = EnumValue
    cassandra_type = 'text'

    def __init__(self, related_persistent_class, enum, **kwargs):
        self._related_persistent_class = related_persistent_class
        self.enum = enum
        super(EnumSetField, self).__init__(related_persistent_class, **kwargs)

class EmailField(UnicodeField):
    def __init__(self, **kwargs):
        super(EmailField, self).__init__(100, **kwargs)


class BooleanField(Field):
    value_class = bool
    cassandra_type = 'boolean'

    def __init__(self, *args, **kwargs):
        super(BooleanField, self).__init__(elixir.Boolean(), **kwargs)


class HttpField(UnicodeField):
    def __init__(self, *args, **kwargs):
        super(HttpField, self).__init__(500, *args, **kwargs)


class EnumType(elixir.TypeDecorator):
    """Base class of Entity type that stores an EnumValue as an Integer."""

    # Subclasses must set the enum.
    enum = None
    
    impl = elixir.Integer
    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        assert(type(value) == EnumValue), "Wrong type for value: %s" % type(value)
        return value.number

    def process_result_value(self, value, dialect):
        # This is a hack, but when adding a column in a migration the default
        # doesn't get set and the value is None.
        # Check for that case here, and assume 0 as a default if needed.
        if value is None:
            return None
        return self.enum[value]

    def copy(self):
        t = EnumType(self.impl)
        t.enum = self.enum
        return t
    

class UUIDType(elixir.TypeDecorator):
    """Base class of Entity type that stores an UUID as an hex string."""

    impl = UUID

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        else:
            return str(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        else:
            return HsnUUID(value)

    def copy(self):
        return UUIDType(self.impl)


class IPAddrType(elixir.TypeDecorator):
    """Base class of Entity type that stores an IPAddress."""

    impl = INET

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        else:
            return str(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        else:
            return unicode(value)

    def copy(self):
        return IPAddrType(self.impl)


class HsnTimeType(elixir.TypeDecorator):
    """Entity type that stores a UTC Integer / 1000."""

    impl = sqlalchemy.types.BigInteger
    def process_bind_param(self, value, dialect):
        from util.when import to_hsntime
        if value is None:
            return -1
        else:
            return to_hsntime(value)

    def process_result_value(self, value, dialect):
        from util.when import from_hsntime
        if value == -1:
            return None
        else:
            return from_hsntime(value)

    def copy(self):
        return HsnTimeType(self.impl)


class HsnDateType(elixir.TypeDecorator):
    """Entity type that stores a Date as an integer."""

    impl = elixir.Integer
    def process_bind_param(self, value, dialect):
        from util.when import to_hsndate
        if value is None:
            return -1
        else:
            return to_hsndate(value)

    def process_result_value(self, value, dialect):
        from util.when import from_hsndate
        if value == -1:
            return None
        else:
            return from_hsndate(value)

    def copy(self):
        return HsnDateType(self.impl)


class TimeType(elixir.TypeDecorator):
    """Entity type that stores a time as timezone independent seconds.
    ex: 9:10:45 --> 9*60*60+10*60+45
    """

    impl = sqlalchemy.types.BigInteger
    def process_bind_param(self, value, dialect):
        from util.when import to_timestorage
        if value is None:
            return -1
        else:
            return to_timestorage(value)

    def process_result_value(self, value, dialect):
        from util.when import from_timestorage
        if value == -1:
            return None
        else:
            return from_timestorage(value)

    def copy(self):
        return TimeType(self.impl)


class DateField(Field):
    value_class = date
    cassandra_type = 'int'

    def __init__(self, **kwargs):
        super(DateField, self).__init__(HsnDateType, **kwargs)

    def cassandra_encode(self, value):
        if value is None:
            return None
        else:
            return to_hsndate(value)

    def cassandra_decode(self, encoded_value):
        if encoded_value is None:
            return None
        else:
            return from_hsndate(encoded_value)
        
class DatetimeField(Field):
    value_class = datetime
    cassandra_type = 'bigint'

    def __init__(self, **kwargs):
        super(DatetimeField, self).__init__(HsnTimeType, **kwargs)

    def cassandra_encode(self, value):
        if value is None:
            return None
        else:
            return to_hsntime(value)

    def cassandra_decode(self, encoded_value):
        if encoded_value is None:
            return None
        else:
            return from_hsntime(encoded_value)

class TimeField(Field):
    value_class = time
    cassandra_type = 'bigint'

    def __init__(self, **kwargs):
        super(TimeField, self).__init__(TimeType, **kwargs)

    def cassandra_encode(self, value):
        if value is None:
            return None
        else:
            return to_timestorage(value)

    def cassandra_decode(self, encoded_value):
        if encoded_value is None:
            return None
        else:
            return from_timestorage(encoded_value)

# These are left over for use as helper properties in the Entity objects.
def nnp_set(enum, attr_name):
    def do_set(self, enum_value):
        assert(enum_value is not None)
        assert(type(enum_value) == EnumValue)
        setattr(self, attr_name, enum_value.number)
    return do_set


def nnp_get(enum, attr_name):
    def do_get(self):
        val = getattr(self, attr_name)
        assert(val is not None)
        return enum[val]
    return do_get


def nnps_set(name_number_set, attr_name, assoc_class, assoc_attr='object'):
    def do_set(self, name_number_pairs):
        setattr(self, attr_name, [])
        for pair in name_number_pairs:
            assoc = assoc_class.create()
            assoc.number=pair.number
            setattr(assoc, assoc_attr, self)
    return do_set


def nnps_get(name_number_set, attr_name):
    def do_get(self):
        return [name_number_set.by_number[assoc.number] for assoc in getattr(self,attr_name)]

    return do_get


def nnps_get_one(name_number_pair, attr_name):
    def do_get_one(self):
        return name_number_pair.number in [assoc.number for assoc in getattr(self,attr_name)]
    return do_get_one


DISALLOWED_FIELD_NAMES = frozenset(['add', 'alter', 'and', 'any', 'apply',
    'asc', 'authorize', 'batch', 'begin', 'by', 'columnfamily', 'create',
    'delete', 'desc', 'drop', 'each_quorum', 'environment', 'failed_loaders',
    'from', 'grant', 'has_temporary_key', 'in', 'index', 'insert', 'into',
    'is_session_entity', 'key', 'keyspace', 'limit', 'load_failed',
    'local_quorum', 'modify', 'norecursive', 'of', 'on', 'one', 'order',
    'primary', 'quorum', 'revoke', 'schema', 'select', 'set', 'table',
    'three', 'token', 'truncate', 'two', 'update', 'use', 'using', 'where',
    'with', 'write_only'] + dir(Persistent) + dir(Entity))
