
import elixir
import elixir.collection
import elixir.entity
from sqlalchemy import MetaData, create_engine
import sqlalchemy.orm

import util.database
from util.log import logger
from backend.tests import TestCase, _setup_module, _teardown_module
from backend.schema import Persistent
from backend.environments.core import Entity
from backend.environments.cache import Cache
from backend import new_active_session, get_active_session
from backend.schema import (UnicodeField, OneToManyField, ManyToOneField,
    UUIDField)
from util.cache import get_redis


db_name = 'test_cache_inclusion_one_to_many'

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

class BridgePersist(Persistent):
    elixir.using_options(tablename='bridge_object',
                         collection=collection)
    uuid = UUIDField(primary_key=True)
    bar = ManyToOneField('BarPersist', inverse='bridge')
    foo1 = ManyToOneField('FooPersist', inverse='bridge1')
    foo2 = ManyToOneField('FooPersist', inverse='bridge2')

class Bridge(Entity):
    pass
Bridge.persistent_class = BridgePersist
Bridge.entity_class = Bridge
BridgePersist.entity_class = Bridge
BridgePersist.persistent_class = BridgePersist

class BarPersist(Persistent):
    elixir.using_options(tablename='bar_object',
                         collection=collection)
    uuid = UUIDField(primary_key=True)
    bridge = OneToManyField('BridgePersist', inverse='bar')
    name1 = UnicodeField()
    name2 = UnicodeField()

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
    bridge1 = OneToManyField('BridgePersist', inverse='foo1')
    bridge2 = OneToManyField('BridgePersist', inverse='foo2')

class Foo(Entity):
    pass
Foo.persistent_class = FooPersist
Foo.entity_class = Foo
FooPersist.entity_class = Foo
FooPersist.persistent_class = FooPersist

class FooCache(Cache):
    entity_class = Foo
    inventory = [
        'uuid',
        ['bridge1', [['bar',['name1']]]],
        ['bridge2', [['bar',['name2']]]]]
    key_template = 'test_cache_inclusion_%s'

logger.error(collection)

#class FooCache1(EnvironmentObjectCache):
#    entity_class = Foo
#    inventory = [
#        'id', 'uuid',
#        ['bridge1', [['bar',['name1']]]],
#        ['bridge2', ['bar']]]
#    key_template = 'test_cache_inclusion_%s'
#
#class FooCache2(EnvironmentObjectCache):
#    entity_class = Foo
#    inventory = [
#        'id', 'uuid',
#        ['bridge1', ['bar']],
#        ['bridge2', [['bar',['name2']]]]]
#    key_template = 'test_cache_inclusion_%s'

class TestCacheInclusionOneToMany(TestCase):

    def test_one_to_many_inclusion(self):
        """Should test that cache will load"""

        # Create Persistent objects.
        foo = Foo.create()
        foo_key = foo.uuid

        bridge = Bridge.create()
        bridge_key = bridge.uuid

        bar = Bar.create()
        bar_key = bar.uuid

        # Set up the bridge, but not the bar.
        bridge.foo1 = foo
        bridge.foo2 = foo

        get_active_session().save(try_later_on_this_thread_first=True, cassandra=False)
        new_active_session()

        #Test for objects in main persistent
        foo_persist = FooPersist.get_by(uuid=foo_key)
        self.assertNotEqual(None, foo_persist)
        bridge_persist = BridgePersist.get_by(uuid=bridge_key)
        self.assertNotEqual(None, bridge_persist)
        bar_persist = BarPersist.get_by(uuid=bar_key)
        self.assertNotEqual(None, bar_persist)

        self.assertTrue(bridge_persist in foo_persist.bridge1)
        self.assertTrue(bridge_persist in foo_persist.bridge2)
        self.assertEqual(bridge_persist.foo1, foo_persist)
        self.assertEqual(bridge_persist.foo2, foo_persist)
        self.assertEqual(0, len(bar_persist.bridge))
        self.assertEqual(None, bridge_persist.bar)

        session.expunge_all()

        # Load (and init) a fresh FooCache, it should not contain a Bar.
        self.assertEqual(None, get_redis().get(FooCache(foo_key).get_redis_key()))
        cache = FooCache(foo_key).get_initialized_cache()
        self.assertNotEqual(None, cache)
        foo_cache = cache.get(Foo, foo_key)
        self.assertNotEqual(None, foo_cache)
        bridge_cache = cache.get(Bridge, bridge_key)
        self.assertNotEqual(None, bridge_cache)
        bar_cache = cache.get(Bar, bar_key)
        self.assertEqual(None, bar_cache)
        self.assertTrue(bridge_cache in foo_cache.bridge1)
        self.assertTrue(bridge_cache in foo_cache.bridge2)
        self.assertEqual(None, bridge_cache.bar)
        # Now that we know everything is set up properly, let's work with
        # the session.

        foo_session = Foo(foo_key)
        foo_session.load([FooCache])
        bridge_session = Bridge(bridge_key)
        self.assertTrue(bridge_session in foo_session.bridge1)
        self.assertTrue(bridge_session in foo_session.bridge2)
        self.assertEqual(None, bridge_session.bar)

        # Here we test whether the session is smart enough to include
        # additional information about Bar.
        # (Note that the information must be available to the session or else
        # it will trigger an async refresh.)
        bar_session = Bar(bar_key)
        bar_session.name1 = u'bar name 1'
        bar_session.name2 = u'bar name 2'
        bridge_session.bar = bar_session

        # (It should know that foo.bridge1.bar only has name1
        # and foo.bridge2.bar only has name2)
        foo_session.save(FooCache)
        get_active_session().save(try_later_on_this_thread_first=True, cassandra=False)
        new_active_session()

        # Load the existing FooCache, it should now contain a Bar.
        self.assertNotEqual(None, get_redis().get(FooCache(foo_key).get_redis_key()))
        cache = FooCache(foo_key)._get_redis()
        self.assertNotEqual(None, cache)
        foo_cache = cache.get(Foo, foo_key)
        self.assertNotEqual(None, foo_cache)
        bridge_cache = cache.get(Bridge, bridge_key)
        self.assertNotEqual(None, bridge_cache)
        bar_cache = cache.get(Bar, bar_key)
        self.assertNotEqual(None, bar_cache)
        self.assertTrue(bridge_cache in foo_cache.bridge1)
        self.assertTrue(bridge_cache in foo_cache.bridge2)
        self.assertTrue(bridge_cache in bar_cache.bridge)
        self.assertEqual(bar_cache.name1, u'bar name 1')
        self.assertEqual(bar_cache.name2, u'bar name 2')

    def test_reverse_attribute_inclusion(self):
        """Should test that cache will save"""
        # Create Persistent objects.
        foo = Foo.create()
        foo_key = foo.uuid

        bridge = Bridge.create()
        bridge_key = bridge.uuid

        bar = Bar.create()
        bar_key = bar.uuid

        get_active_session().save(try_later_on_this_thread_first=True, cassandra=False)
        new_active_session()

        #Test for objects in main persistent
        foo_persist = FooPersist.get_by(uuid=foo_key)
        bridge_persist = BridgePersist.get_by(uuid=bridge_key)
        bar_persist = BarPersist.get_by(uuid=bar_key)
        self.assertNotEqual(None, foo_persist)
        self.assertNotEqual(None, bridge_persist)
        self.assertNotEqual(None, bar_persist)

        self.assertEqual(0, len(foo_persist.bridge1))
        self.assertEqual(0, len(foo_persist.bridge2))
        self.assertEqual(bridge_persist.foo1, None)
        self.assertEqual(bridge_persist.foo2, None)
        self.assertEqual(0, len(bar_persist.bridge))
        self.assertEqual(None, bridge_persist.bar)

        session.expunge_all()

        # Load (and init) a fresh FooCache
        self.assertEqual(None, get_redis().get(FooCache(foo_key).get_redis_key()))
        cache = FooCache(foo_key).get_initialized_cache()
        self.assertNotEqual(None, cache)

        foo_cache = cache.get(Foo, foo_key)
        bridge_cache = cache.get(Bridge, bridge_key)
        bar_cache = cache.get(Bar, bar_key)

        #it should not contain a Bar or a Bridge.
        self.assertNotEqual(None, foo_cache)
        self.assertEqual(None, bridge_cache)
        self.assertEqual(None, bar_cache)
        self.assertEqual(0, len(foo_cache.bridge1))
        self.assertEqual(0, len(foo_cache.bridge2))

        new_active_session()

        # Now that we know everything is set up properly, let's work with
        # the session.
        foo_session = Foo(foo_key)
        foo_session.load([FooCache])
        bridge_session = Bridge(bridge_key)
        bar_session = Bar(bar_key)
        self.assertEqual(0, len(foo_session.bridge1))
        self.assertEqual(0, len(foo_session.bridge2))

        #Assign names to Bar
        bar_session.name1 = u'bar name 1'
        bar_session.name2 = u'bar name 2'

        # PRE-FIX NOTE (MAV):
        #If you assign bridge_session.foo1 = foo_session first the test will PASS
        #If you assign bridge_session.bar = bar_session first the test will FAIL

        #Assign Bar to bridge
        bridge_session.bar = bar_session

        #Assign Foo to bridge
        bridge_session.foo1 = foo_session

        #It should save Bridge to foo.bridge1
        # and bar to bridge.bar
        # and name1 to bar
        # so that we can access name1 like so:
        # foo.bridge1.bar.name1
        foo.save(FooCache)
        get_active_session().save(try_later_on_this_thread_first=True, cassandra=False)
        new_active_session()

        #Check cache
        # Load the existing FooCache.
        # It should now contain a Bridge, a Bar and the name1 attribute of the Bar
        self.assertNotEqual(None, get_redis().get(FooCache(foo_key).get_redis_key()))
        cache = FooCache(foo_key)._get_redis()
        foo_cache = cache.get(Foo, foo_key)
        bridge_cache = cache.get(Bridge, bridge_key)
        bar_cache = cache.get(Bar, bar_key)

        self.assertNotEqual(None, cache)
        self.assertNotEqual(None, foo_cache)
        self.assertNotEqual(None, bridge_cache)
        self.assertNotEqual(None, bar_cache)
        self.assertTrue(bridge_cache in foo_cache.bridge1)
        self.assertTrue(bridge_cache in bar_cache.bridge)
        self.assertEqual(bar_cache.name1, u'bar name 1')
        self.assertEqual(foo_cache.bridge1.get_one().bar.name1, u'bar name 1')
