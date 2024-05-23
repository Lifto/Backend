
from uuid import UUID

import elixir
import elixir.collection
import elixir.entity
from sqlalchemy import MetaData, create_engine
import sqlalchemy.orm

import util.database
from util.cache import get_redis
from backend.tests import TestCase, _setup_module, _teardown_module
from backend.schema import Persistent, Enum, EnumValue
from backend.environments.core import Entity
from backend.environments.cache import Cache
from backend import new_active_session, save, load, MainPersistent
from backend.schema import UnicodeField, UUIDField, StringField, EnumField, \
    IntegerField, FloatField, BooleanField

db_name = 'test_zset'

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
    int_field = IntegerField()
    float_field = FloatField()
    boolean_field = BooleanField()

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
        'float_field', 'boolean_field'
    ]
    key_template = 'test_general_cache_%s'

class TestZSetCache(TestCase):

    def test_zset_cache(self):
        """ZSetCache."""
        from util.log import logger

        # Create Persistent object.
        foo = Foo.create()
        foo_key = foo.uuid
        foo.str_field = 'abcd'
        foo.uni_field = u'abcd'
        foo.int_field = 22
        foo.float_field = 3.14159265
        foo.enum_field = FooEnum[u'grob']
        foo.boolean_field = True

        save(try_later_on_this_thread_first=True, cassandra=False)
        new_active_session()

        self.assertTrue(True)
