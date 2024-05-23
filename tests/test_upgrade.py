
from uuid import UUID

import elixir
import elixir.collection
import elixir.entity
from sqlalchemy import MetaData, create_engine
import sqlalchemy.orm

import backend
from backend.exceptions import RedisLoadError
import util.database
from util.log import logger
from backend.tests import TestCase, _setup_module, _teardown_module
from backend.schema import Persistent
from backend.environments.core import Entity
from backend.environments.cache import Cache
from backend import get_active_session
from backend.schema import UnicodeField, UUIDField
from util.cache import get_redis

db_name = 'test_upgrade'

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

class FooPersist(Persistent):
    elixir.using_options(tablename='foo_object', collection=collection)
    uuid = UUIDField(primary_key=True)
    old_field = UnicodeField()
    new_field = UnicodeField()

class Foo(Entity):
    pass
Foo.persistent_class = FooPersist
Foo.entity_class = Foo
FooPersist.entity_class = Foo
FooPersist.persistent_class = FooPersist

class FooCache(Cache):
    entity_class = Foo
    shallow = True
    version = 1
    inventory = ['uuid', 'old_field', 'new_field']

    @classmethod
    def upgrade(cls, encoding):
        logger.debug("UPGRADE--------------------------- %s" % encoding)
        if encoding.get(backend.environments.cache.SHALLOW_VERSION_KEY, 0) == '0':
            key = UUID(encoding['uuid'])
            encoding['new_field'] = encoding['old_field']
            encoding[backend.environments.cache.SHALLOW_VERSION_KEY] = '1'
            pipeline = get_redis().pipeline()
            pipeline.hmset(cls._get_redis_key(key), {
                'new_field': encoding['old_field'],
                backend.environments.cache.SHALLOW_VERSION_KEY: '1'
            })
            return encoding, pipeline
        raise RedisLoadError

class TestShallow(TestCase):

    def test_cache_should_upgrade(self):
        """Cache should upgrade"""

        # Make in id for our object
        foo_key = backend.generate_id()

        # Make an old version of the Cache.
        vals = {
            backend.environments.cache.SHALLOW_INIT_KEY: True,
            backend.environments.cache.SHALLOW_VERSION_KEY: '0',
            'uuid': FooCache.serialize_value(foo_key, Foo.get_attribute('uuid')),
            'old_field': u'same_as_old'
        }

        # Put it in Redis.
        get_redis().hmset(FooCache(foo_key).get_redis_key(), vals)

        # Load the object via Backend.
        foo = Foo(foo_key)
        foo.load(FooCache)

        # Confirm that the upgrade worked.  (Note: We faked this object, it's
        # not in the SQL, so if it doesn't load from redis and upgrade there
        # will be an error.)
        self.assertEqual(foo.old_field, u'same_as_old')
        self.assertEqual(foo.new_field, u'same_as_old')

        # Call save, this should save the upgraded version of the cache.
        get_active_session().save(try_later_on_this_thread_first=True, cassandra=False)

        # Get the cache from redis and inspect its contents.
        cache = get_redis().hgetall(FooCache(foo_key).get_redis_key())
        self.assertEqual(cache[backend.environments.cache.SHALLOW_INIT_KEY],
                         'True')
        self.assertEqual(cache[backend.environments.cache.SHALLOW_VERSION_KEY],
                         '1')
        self.assertEqual(cache['old_field'], u'same_as_old')
        self.assertEqual(cache['new_field'], u'same_as_old')
