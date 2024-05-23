
import elixir
import elixir.collection
import elixir.entity
from sqlalchemy import MetaData, create_engine
import sqlalchemy.orm

import util.database
from backend.tests import TestCase, _setup_module, _teardown_module
from backend.schema import Persistent, Enum
from backend.environments.core import Entity
from backend.environments.cache import Cache
from backend import new_active_session, get_active_session
from backend.schema import UnicodeField, UUIDField, StringField, EnumField, \
    IncrementField, FloatField
from util.cache import get_redis

db_name = 'test_shallow'

engine = create_engine(util.database.get_db_url(db_name))
metadata = MetaData()
metadata.bind = engine
session = sqlalchemy.orm.scoped_session(sqlalchemy.orm.sessionmaker())
session.bind = engine
collection = elixir.collection.EntityCollection()

#Required for Elixir Entity Setup, do not remove
__session__ = session
__metadata__ = metadata

def setup_module():
    cache_list = [FooCache]
    loader_list = []

    _setup_module(db_name, collection, metadata, session, cache_list, loader_list)

def teardown_module():
    _teardown_module(db_name, collection, metadata, session, engine)

#Model Classes needed for Test
#Define and Setup Test Database

FooEnum = Enum(u'FooEnum', [u'frob', u'grob', u'blob'])

class FooPersist(Persistent):
    elixir.using_options(tablename='foo_object', collection=collection)
    uuid = UUIDField(primary_key=True)
    str_field = StringField()
    uni_field = UnicodeField()
    enum_field = EnumField(FooEnum)
    int_field = IncrementField()
    float_field = FloatField()

class Foo(Entity):
    pass
Foo.persistent_class = FooPersist
Foo.entity_class = Foo
FooPersist.entity_class = Foo
FooPersist.persistent_class = FooPersist

class FooCache(Cache):
    entity_class = Foo
    shallow = True
    inventory = [
        'uuid', 'str_field', 'uni_field', 'enum_field', 'int_field',
        'float_field'
    ]
    key_template = 'test_shallow_cache_%s'

def save():
    get_active_session().save(try_later_on_this_thread_first=True, cassandra=False)
    new_active_session()

class TestShallow(TestCase):

    def test_shallow_cache_on_create(self):
        """Cache should save at create"""

        # Create Persistent object.
        foo = Foo.create()
        foo_key = foo.uuid
        foo.str_field = 'abcd'
        foo.uni_field = u'abcd'
        foo.increment('int_field', 22)
        foo.float_field = 3.14159265
        foo.enum_field = FooEnum[u'grob']

        foo.save(FooCache)

        get_active_session().save(try_later_on_this_thread_first=True, cassandra=False)
        self.check_sql_and_cache(foo_key)

    def test_shallow_cache_on_refresh(self):
        """Cache should refresh from SQL"""

        # Create Persistent object.
        foo = Foo.create()
        foo_key = foo.uuid
        foo.str_field = 'abcd'
        foo.uni_field = u'abcd'
        foo.increment('int_field', 22)
        foo.float_field = 3.14159265
        foo.enum_field = FooEnum[u'grob']

        save()

        foo = Foo(foo_key)
        foo.load(FooCache)

        get_active_session().save(try_later_on_this_thread_first=True, cassandra=False)

        self.check_sql_and_cache(foo_key)

    def test_shallow_increment_cache_on_create(self):
        """Cache should create on create"""

        # Create Persistent object.
        foo = Foo.create()
        foo_key = foo.uuid
        foo.str_field = 'abcd'
        foo.uni_field = u'abcd'
        foo.increment('int_field', 21)
        foo.float_field = 3.14159265
        foo.enum_field = FooEnum[u'grob']

        self.assertEqual(21, foo.int_field)
        foo.increment('int_field', 1)
        self.assertEqual(22, foo.int_field)

        foo.save(FooCache)

        get_active_session().save(try_later_on_this_thread_first=True, cassandra=False)

        self.check_sql_and_cache(foo_key)

    def test_shallow_increment_cache_on_refresh(self):
        """Cache should refresh from SQL"""

        # Create Persistent object.
        foo = Foo.create()
        foo_key = foo.uuid
        foo.str_field = 'abcd'
        foo.uni_field = u'abcd'
        foo.increment('int_field', 21)
        foo.float_field = 3.14159265
        foo.enum_field = FooEnum[u'grob']

        save()

        foo = Foo(foo_key)
        foo.load(FooCache)

        self.assertEqual(foo.int_field, 21)
        foo.increment('int_field', 1)
        self.assertEqual(foo.int_field, 22)

        get_active_session().save(try_later_on_this_thread_first=True, cassandra=False)

        self.check_sql_and_cache(foo_key)

    def check_sql_and_cache(self, foo_key):
        new_active_session()
        foo = FooPersist.get_by(uuid=foo_key)
        self.assertNotEqual(None, foo)
        self.assertEqual(foo.str_field, 'abcd')
        self.assertEqual(foo.uni_field, u'abcd')
        self.assertEqual(foo.int_field, 22)
        self.assertEqual(foo.float_field, 3.14159265)
        self.assertEqual(foo.enum_field, FooEnum[u'grob'])

        self.assertEqual(None, get_active_session().get(Foo, foo_key))
        encoding = get_redis().hgetall(FooCache(foo_key).get_redis_key())
        self.assertTrue(encoding)
        FooCache.patch_from_encoding(encoding)
        foo = Foo(foo_key)
        self.assertEqual(foo.str_field, 'abcd')
        self.assertEqual(foo.uni_field, u'abcd')
        self.assertEqual(foo.int_field, 22)
        self.assertEqual(foo.float_field, 3.14159265)
        self.assertEqual(foo.enum_field, FooEnum[u'grob'])
