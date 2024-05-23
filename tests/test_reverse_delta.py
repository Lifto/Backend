
import elixir
import elixir.collection
import elixir.entity
from sqlalchemy import MetaData, create_engine
import sqlalchemy.orm

import util.database
from backend.tests import TestCase, _setup_module, _teardown_module
from backend.schema import Persistent
from backend.environments.core import Entity
from backend.environments.cache import Cache
from backend import new_active_session, get_active_session
from backend.schema import ManyToOneField, OneToManyField, UUIDField
from util.cache import get_redis


# What if we wanted to preserve existing elixir at this point and pop later?
db_name = 'test_cache_reverse_delta'

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

class BarPersist(Persistent):
    elixir.using_options(tablename='bar_object',
                         collection=collection)
    uuid = UUIDField(primary_key=True)
    foo = ManyToOneField('FooPersist', inverse='bar')

class Bar(Entity):
    pass
Bar.persistent_class = BarPersist
Bar.entity_class = Bar
BarPersist.entity_class = Bar
BarPersist.persistent_class = BarPersist

class FooPersist(Persistent):
    elixir.using_options(tablename='foo_object',
                         collection=collection)
    uuid = UUIDField(primary_key=True)
    bar = OneToManyField('BarPersist', inverse='foo')

class Foo(Entity):
    pass
Foo.persistent_class = FooPersist
Foo.entity_class = Foo
FooPersist.entity_class = Foo
FooPersist.persistent_class = FooPersist

class FooCache(Cache):
    entity_class = Foo
    inventory = ['uuid', 'bar']
    key_template = 'test_cache_inclusion_%s'

def save():
    get_active_session().save(try_later_on_this_thread_first=True, cassandra=False)
    new_active_session()

class TestCacheReverseDelta(TestCase):

    def test_reverse_delta(self):
        """Should test that cache will load"""

        # Create Persistent objects.
        foo = Foo.create()
        foo_key = foo.uuid

        bar_1 = Bar.create()
        bar_key_1 = bar_1.uuid

        bar_2 = Bar.create()
        bar_key_2 = bar_2.uuid

        save()

        # Test for objects in main persistent
        foo_persist = FooPersist.get_by(uuid=foo_key)
        self.assertNotEqual(None, foo_persist)
        bar_persist_1 = BarPersist.get_by(uuid=bar_key_1)
        self.assertNotEqual(None, bar_persist_1)
        bar_persist_2 = BarPersist.get_by(uuid=bar_key_2)
        self.assertNotEqual(None, bar_persist_2)

        self.assertEqual(0, len(foo_persist.bar))
        self.assertEqual(None, bar_persist_1.foo)
        self.assertEqual(None, bar_persist_2.foo)

        session.expunge_all()

        # Load (and init) a fresh FooCache, it should not contain a Bar.
        self.assertEqual(None, get_redis().get(FooCache(foo_key).get_redis_key()))
        cache = FooCache(foo_key).get_initialized_cache()
        self.assertNotEqual(None, cache)
        foo_cache = cache.get(Foo, foo_key)
        self.assertNotEqual(None, foo_cache)
        self.assertEqual(0, len(foo_cache.bar))
        bar_cache_1 = cache.get(Bar, bar_key_1)
        self.assertEqual(None, bar_cache_1)
        bar_cache_2 = cache.get(Bar, bar_key_2)
        self.assertEqual(None, bar_cache_2)

        # Now that we know everything is set up properly, let's work with
        # the session.
        foo_session = Foo(foo_key)
        foo_session.load([FooCache])
        self.assertEqual(0, len(foo_session.bar))

        # Here we test whether the session is smart enough to include
        # a delta on a Bar
        bar_session_1 = Bar(bar_key_1)
        bar_session_2 = Bar(bar_key_2)

        bar_session_1.foo = foo_session

        def_env = get_active_session()
        def_env.save(def_env.loaded_environments, cassandra=False)
        new_active_session()

        # Load the existing FooCache, it should now contain the Bars.
        self.assertNotEqual(None, get_redis().get(FooCache(foo_key).get_redis_key()))
        cache = FooCache(foo_key)._get_redis()
        self.assertNotEqual(None, cache)
        foo_cache = cache.get(Foo, foo_key)
        self.assertNotEqual(None, foo_cache)
        bar_cache_1 = cache.get(Bar, bar_key_1)
        self.assertNotEqual(None, bar_cache_1)

        self.assertTrue(bar_cache_1 in foo_cache.bar)
        self.assertEqual(1, len(foo_cache.bar))
        self.assertEqual(foo_cache, bar_cache_1.foo)
