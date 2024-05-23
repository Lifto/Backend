
from uuid import UUID

from datetime import datetime, date, time
import elixir
import elixir.collection
import elixir.entity
from sqlalchemy import MetaData, create_engine
import sqlalchemy.orm

from backend import Cassandra, \
                    generate_id, \
                    get_active_session, \
                    load, \
                    MainPersistent, \
                    new_active_session

from backend.environments.core import Entity
from backend.environments.cache import Cache
from backend.exceptions import NullEnvironmentError
from backend.loader import CompositeKeyLoader, InventoryLoader
from backend.schema import BooleanField, \
                           DateField, \
                           DatetimeField, \
                           Enum, \
                           EnumField, \
                           EnumValue, \
                           FakeManyToOneField, \
                           FloatField, \
                           IncrementField, \
                           IntegerField, \
                           ManyToOneField, \
                           OneToManyField, \
                           OneToOneField, \
                           Persistent, \
                           StringField, \
                           TimeField, \
                           UnicodeField, \
                           UUIDField
from backend.tests import TestCase, _setup_module, _teardown_module
import util.cassandra
import hsn.util.database
from util.cache import get_redis
from util.when import to_hsntime, to_timestorage, to_hsndate

db_name = 'test_general'

engine = create_engine(hsn.util.database.get_db_url(db_name))
metadata = MetaData()
metadata.bind = engine
session = sqlalchemy.orm.scoped_session(sqlalchemy.orm.sessionmaker())
session.bind = engine

collection = elixir.collection.EntityCollection()

#Required for Elixir Entity Setup, do not remove
__session__ = session
__metadata__ = metadata

def setup_module():
    cache_list = [FooShallowCache, FooDeepCache]
    loader_list = [FooLoader]

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
    enum_field = EnumField(FooEnum, default=FooEnum[1])
    int_field = IntegerField()
    inc1_field = IncrementField(default=0)
    inc2_field = IncrementField(default=2)
    inc3_field = IncrementField(default=None)
    float_field = FloatField()
    boolean_field = BooleanField()
    datetime_field = DatetimeField()
    time_field = TimeField()
    date_field = DateField()
    fake_many_to_one = FakeManyToOneField('BarPersist', inverse='one_to_one')
    many_to_one = ManyToOneField('BarPersist', inverse='one_to_many')

class Foo(Entity):
    pass
Foo.persistent_class = FooPersist
Foo.entity_class = Foo
FooPersist.entity_class = Foo
FooPersist.persistent_class = FooPersist

class BarPersist(Persistent):
    elixir.using_options(tablename='bar_object', collection=collection)
    uuid = UUIDField(primary_key=True)
    one_to_one = OneToOneField('FooPersist', inverse='fake_many_to_one')
    one_to_many = OneToManyField('FooPersist', inverse='many_to_one')

class Bar(Entity):
    pass
Bar.persistent_class = BarPersist
Bar.entity_class = Bar
BarPersist.entity_class = Bar
BarPersist.persistent_class = BarPersist

class CountOnlyPersist(Persistent):
    elixir.using_options(tablename='countonly_object', collection=collection)
    uuid = UUIDField(primary_key=True)
    inc1 = IncrementField(default=2)
    inc2 = IncrementField(default=None)

class CountOnly(Entity):
    pass
CountOnly.persistent_class = CountOnlyPersist
CountOnly.entity_class = CountOnly
CountOnlyPersist.entity_class = CountOnly
CountOnlyPersist.persistent_class = CountOnlyPersist

class KeyOnlyPersist(Persistent):
    elixir.using_options(tablename='keyonly_object', collection=collection)
    uuid = UUIDField(primary_key=True)

class KeyOnly(Entity):
    pass
KeyOnly.persistent_class = KeyOnlyPersist
KeyOnly.entity_class = KeyOnly
KeyOnlyPersist.entity_class = KeyOnly
KeyOnlyPersist.persistent_class = KeyOnlyPersist

class FooShallowCache(Cache):
    entity_class = Foo
    shallow = True
    inventory = [
        'uuid', 'str_field', 'uni_field', 'enum_field', 'int_field',
        'float_field', 'boolean_field', 'inc1_field', 'inc2_field',
        'inc3_field', 'fake_many_to_one', 'many_to_one',# 'datetime_field',
        #'date_field', 'time_field'
    ]
    key_template = 'test_general_shallow_cache_%s'

class FooDeepCache(Cache):
    entity_class = Foo
    inventory = FooShallowCache.inventory
    key_template = 'test_general_deep_cache_%s'

class FooLoader(InventoryLoader):
    entity_class = Foo
    inventory = FooShallowCache.inventory


def postgresql_execute(stmt):
    from sqlalchemy import create_engine
    import psycopg2
    engine = create_engine(hsn.util.database.get_db_url(db_name), echo=False)
    engine.raw_connection().set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    engine.text(stmt).execute()


ACCELERATED_SAVE = True
def save():
    if ACCELERATED_SAVE:
        get_active_session().accelerated_save = True
    get_active_session().save(try_later_on_this_thread_first=True)
    new_active_session()


class TestGeneral(TestCase):
    collection = collection

    def test_sql_increment_orm_save(self):
        """Increment ORM save."""
        global ACCELERATED_SAVE
        ACCELERATED_SAVE = False
        self._t_increment()

    def test_sql_increment_accelerated_save(self):
        """Increment accelerated save."""
        global ACCELERATED_SAVE
        ACCELERATED_SAVE = True
        self._t_increment()

    def _t_increment(self):
        self.reset_cassandra()
        FooShallowCache.clear_all()
        FooDeepCache.clear_all()

        foo = Foo.create()
        foo_key = foo.key

        save()

        foo_persist = FooPersist.get_by(uuid=foo_key)

        self.assertEqual(0, foo_persist.inc1_field)
        self.assertEqual(2, foo_persist.inc2_field)
        self.assertEqual(None, foo_persist.inc3_field)

        # Let's try an increment.

        foo = Foo(foo_key)
        foo.increment('inc2_field', 3)
        foo.increment('inc3_field', 15)

        save()

        foo_persist = FooPersist.get_by(uuid=foo_key)

        self.assertEqual(0, foo_persist.inc1_field)
        self.assertEqual(5, foo_persist.inc2_field)
        self.assertEqual(15, foo_persist.inc3_field)

        foo = Foo(foo_key)
        foo.load(FooShallowCache)

        self.assertEqual(0, foo.inc1_field)
        self.assertEqual(5, foo.inc2_field)
        self.assertEqual(15, foo.inc3_field)

        new_active_session()

        foo = Foo(foo_key)
        foo.load(FooDeepCache)

        self.assertEqual(0, foo.inc1_field)
        self.assertEqual(5, foo.inc2_field)
        self.assertEqual(15, foo.inc3_field)

        new_active_session()

        foo = Foo(foo_key)
        foo.load(FooLoader)
        self.assertEqual(0, foo.inc1_field)
        self.assertEqual(5, foo.inc2_field)
        self.assertEqual(15, foo.inc3_field)

        # Can we do 0 and negative increments?
        new_active_session()

        foo = Foo(foo_key)
        foo.load(MainPersistent)
        foo.increment('inc1_field', -2)
        foo.increment('inc2_field', 0)
        foo.save(FooShallowCache)
        foo.save(FooDeepCache)

        save()

        foo_persist = FooPersist.get_by(uuid=foo_key)
        self.assertEqual(-2, foo_persist.inc1_field)
        self.assertEqual(5, foo_persist.inc2_field)
        self.assertEqual(15, foo_persist.inc3_field)

        new_active_session()

        foo = Foo(foo_key)
        foo.load(MainPersistent)
        self.assertEqual(-2, foo.inc1_field)
        self.assertEqual(5, foo.inc2_field)
        self.assertEqual(15, foo.inc3_field)

        foo = Foo(foo_key)
        foo.load(FooShallowCache)
        self.assertEqual(-2, foo.inc1_field)
        self.assertEqual(5, foo.inc2_field)
        self.assertEqual(15, foo.inc3_field)

        foo = Foo(foo_key)
        foo.load(FooDeepCache)
        self.assertEqual(-2, foo.inc1_field)
        self.assertEqual(5, foo.inc2_field)
        self.assertEqual(15, foo.inc3_field)

        # I took this out because this is no longer the case.
        # Nones are Nones.
        # XXX TAKE THIS BACK OUT.  We made a hack so that Nones are read
        # as 0.  This has the same issues that Enum has.

        # Let's simulate a migration where inc2_field appears as a None.
        # Note: This is left in the test but it is legacy now.  If you
        # load a None it appears as a None.  If you migrate in a field
        # to an existing table you must loop over the table and set your
        # defaults.

        foo_persist.inc1_field = None

        import backend.core
        backend.core.commit_session()
        backend.core.remove_session()

        foo_persist = FooPersist.get_by(uuid=foo_key)

        self.assertEqual(None, foo_persist.inc1_field)

        # Now let's load the Cache and see what the session shows us.

        new_active_session()
        FooShallowCache.clear_all()
        foo = Foo(foo_key)
        foo.load(FooShallowCache)

        self.assertEqual(0, foo.inc1_field)
        save()
        foo = Foo(foo_key)
        foo.load(FooShallowCache)

        self.assertEqual(0, foo.inc1_field)

        foo.increment('inc1_field', 11)
        foo.save(FooShallowCache)
        save()

        foo = Foo(foo_key)
        foo.load(FooShallowCache)

        self.assertEqual(11, foo.inc1_field)

    def test_basic_orm_save(self):
        """Basic ORM save."""
        global ACCELERATED_SAVE
        ACCELERATED_SAVE = False
        self._t_basic()

    def test_basic_accelerated_save(self):
        """Basic accelerated save."""
        global ACCELERATED_SAVE
        ACCELERATED_SAVE = True
        self._t_basic()

    def _t_basic(self):
        """Each field type should save and load."""
        self.reset_cassandra()
        FooShallowCache.clear_all()
        FooDeepCache.clear_all()
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

        bar = Bar.create()
        bar_key = bar.uuid

        save()

        foo = Foo(foo_key)
        bar = Bar(bar_key)

        # We're gonna test some illegal operations here.
        raised = False
        try:
            bar.one_to_many = set()
        except Exception:
            raised = True
        self.assertTrue(raised)

        raised = False
        try:
            bar.one_to_many = set([foo])
        except Exception:
            raised = True
        self.assertTrue(raised)

        raised = False
        try:
            bar.one_to_many = None
        except Exception:
            raised = True
        self.assertTrue(raised)

        new_active_session()

        foo = Foo(foo_key)
        bar = Bar(bar_key)

        # Let's test that if we assign the non-key side of a OneToOne
        # it creates the reverse SetAttribute delta.
        bar.one_to_one = foo
        delta = get_active_session().deltas[-1]
        self.assertEqual(delta.entity, foo)
        self.assertEqual(delta.value, bar)
        self.assertEqual(delta.attribute, foo.get_attribute('fake_many_to_one'))

        save()

        # Test that we can load these values back from MainPersistent.
        foo = Foo(foo_key)
        bar = Bar(bar_key)
        foo.load(MainPersistent)

        self.assertEqual(type(foo.uuid), UUID)
        self.assertEqual(foo.uuid, foo_key)
        self.assertEqual(type(foo.str_field), str)
        self.assertEqual(foo.str_field, 'abcd')
        self.assertEqual(type(foo.uni_field), unicode)
        self.assertEqual(foo.uni_field, u'abcd')
        self.assertEqual(type(foo.int_field), int)
        self.assertEqual(foo.int_field, 22)
        self.assertEqual(type(foo.float_field), float)
        self.assertEqual(foo.float_field, 3.14159265)
        self.assertEqual(type(foo.enum_field), EnumValue)
        self.assertEqual(foo.enum_field, FooEnum[u'grob'])
        self.assertEqual(foo.entity_class.get_attribute('boolean_field').value_class, bool)
        self.assertEqual(type(foo.boolean_field), bool)
        self.assertEqual(foo.boolean_field, True)
        self.assertEqual(type(foo.fake_many_to_one), Bar)
        self.assertEqual(foo.fake_many_to_one, bar)

        save()

        # Save the FooCache.
        foo = Foo(foo_key)
        foo.save(FooShallowCache)

        save()

        # Test the FooShallowCache.
        foo = Foo(foo_key)
        bar = Bar(bar_key)
        foo.load(FooShallowCache)

        self.assertEqual(type(foo.uuid), UUID)
        self.assertEqual(foo.uuid, foo_key)
        self.assertEqual(type(foo.str_field), str)
        self.assertEqual(foo.str_field, 'abcd')
        self.assertEqual(type(foo.uni_field), unicode)
        self.assertEqual(foo.uni_field, u'abcd')
        self.assertEqual(type(foo.int_field), int)
        self.assertEqual(foo.int_field, 22)
        self.assertEqual(type(foo.float_field), float)
        self.assertEqual(foo.float_field, 3.14159265)
        self.assertEqual(type(foo.enum_field), EnumValue)
        self.assertEqual(foo.enum_field, FooEnum[u'grob'])
        self.assertEqual(foo.entity_class.get_attribute('boolean_field').value_class, bool)
        self.assertEqual(type(foo.boolean_field), bool)
        self.assertEqual(foo.boolean_field, True)
        self.assertEqual(type(foo.fake_many_to_one), Bar)
        self.assertEqual(foo.fake_many_to_one, bar)

        save()

        self.assertEqual([], load())
        self.assertEqual([], load(FooShallowCache(foo_key)))
        self.assertEqual([], load(FooShallowCache(foo_key)))
        self.assertEqual([], load([FooShallowCache(foo_key), MainPersistent]))
        r = get_redis()
        p = r.pipeline()
        p.get('foobar')
        self.assertEqual([None], load(pipeline=p))
        r = get_redis()
        p = r.pipeline()
        p.get('foobar')
        self.assertEqual([None], load(FooShallowCache(foo_key), pipeline=p))
        r = get_redis()
        p = r.pipeline()
        p.get('foobar')
        self.assertEqual([None], load(MainPersistent, pipeline=p))

        # Test the FooDeepCache.
        foo = Foo(foo_key)
        foo.load(MainPersistent)
        foo.save(FooDeepCache)
        save()

        foo = Foo(foo_key)
        bar = Bar(bar_key)
        foo.load(FooDeepCache)

        self.assertEqual(type(foo.uuid), UUID)
        self.assertEqual(foo.uuid, foo_key)
        self.assertEqual(type(foo.str_field), str)
        self.assertEqual(foo.str_field, 'abcd')
        self.assertEqual(type(foo.uni_field), unicode)
        self.assertEqual(foo.uni_field, u'abcd')
        self.assertEqual(type(foo.int_field), int)
        self.assertEqual(foo.int_field, 22)
        self.assertEqual(type(foo.float_field), float)
        self.assertEqual(foo.float_field, 3.14159265)
        self.assertEqual(type(foo.enum_field), EnumValue)
        self.assertEqual(foo.enum_field, FooEnum[u'grob'])
        self.assertEqual(foo.entity_class.get_attribute('boolean_field').value_class, bool)
        self.assertEqual(type(foo.boolean_field), bool)
        self.assertEqual(foo.boolean_field, True)
        self.assertEqual(type(foo.fake_many_to_one), Bar)
        self.assertEqual(foo.fake_many_to_one, bar)

        save()

        self.assertEqual([], load())
        self.assertEqual([], load(FooDeepCache(foo_key)))
        self.assertEqual([], load(FooDeepCache(foo_key)))
        self.assertEqual([], load([FooDeepCache(foo_key), MainPersistent]))
        r = get_redis()
        p = r.pipeline()
        p.get('foobar')
        self.assertEqual([None], load(pipeline=p))
        r = get_redis()
        p = r.pipeline()
        p.get('foobar')
        self.assertEqual([None], load(FooDeepCache(foo_key), pipeline=p))
        r = get_redis()
        p = r.pipeline()
        p.get('foobar')
        self.assertEqual([None], load(MainPersistent, pipeline=p))

        # Create another Persistent object.
        foo = Foo.create()
        foo_key = foo.uuid
        foo.str_field = 'abcd'
        foo.uni_field = u'abcd'
        foo.int_field = 22
        foo.float_field = 3.14159265
        foo.enum_field = FooEnum[u'grob']
        foo.boolean_field = True

        bar = Bar.create()
        bar_key = bar.uuid

        save()

        foo = Foo(foo_key)
        foo_key = foo.uuid
        foo.str_field = None
        foo.uni_field = None
        foo.int_field = None
        foo.float_field = None
        foo.enum_field = None
        foo.boolean_field = None

        save()

        # Test that we can load these values back from MainPersistent.
        foo = Foo(foo_key)
        bar = Bar(bar_key)
        foo.load(MainPersistent)

        self.assertEqual(type(foo.uuid), UUID)
        self.assertEqual(foo.uuid, foo_key)
        self.assertEqual(foo.str_field, None)
        self.assertEqual(foo.uni_field, None)
        self.assertEqual(foo.int_field, None)
        self.assertEqual(foo.float_field, None)
        # There is a hack on this now.
#        self.assertEqual(foo.enum_field, None)
        self.assertEqual(foo.enum_field, FooEnum[0])
        self.assertEqual(foo.boolean_field, None)
        self.assertEqual(foo.fake_many_to_one, None)

        FooDeepCache.clear_all()
        FooShallowCache.clear_all()

        new_active_session()

        # Test the FooShallowCache.
        foo = Foo(foo_key)
        bar = Bar(bar_key)
        foo.load(FooShallowCache)

        self.assertEqual(type(foo.uuid), UUID)
        self.assertEqual(foo.uuid, foo_key)
        self.assertEqual(foo.str_field, None)
        self.assertEqual(foo.uni_field, None)
        self.assertEqual(foo.int_field, None)
        self.assertEqual(foo.float_field, None)
        # There is a hack on this now.
#        self.assertEqual(foo.enum_field, None)
        self.assertEqual(foo.enum_field, FooEnum[0])
        self.assertEqual(foo.boolean_field, None)
        self.assertEqual(foo.fake_many_to_one, None)

        save()

        # Test the FooShallowCache.
        foo = Foo(foo_key)
        bar = Bar(bar_key)
        foo.load(FooShallowCache)

        self.assertEqual(type(foo.uuid), UUID)
        self.assertEqual(foo.uuid, foo_key)
        # xxx this is broken, please fix it.  need a new serializer for
        # shallow cache.
        self.assertEqual(foo.str_field, 'None')
        # xxx this is broken, please fix it.
        self.assertEqual(foo.uni_field, u'')#None)
        self.assertEqual(foo.int_field, None)
        self.assertEqual(foo.float_field, None)
        # There is a hack on this now.
#        self.assertEqual(foo.enum_field, None)
        self.assertEqual(foo.enum_field, FooEnum[0])
        self.assertEqual(foo.boolean_field, None)
        self.assertEqual(foo.fake_many_to_one, None)

        save()

        # Test the FooDeepCache.
        foo = Foo(foo_key)
        bar = Bar(bar_key)
        foo.load(FooDeepCache)

        self.assertEqual(type(foo.uuid), UUID)
        self.assertEqual(foo.uuid, foo_key)
        self.assertEqual(foo.str_field, None)
        self.assertEqual(foo.uni_field, None)
        self.assertEqual(foo.int_field, None)
        self.assertEqual(foo.float_field, None)
        # There is a hack on this now.
#        self.assertEqual(foo.enum_field, None)
        self.assertEqual(foo.enum_field, FooEnum[0])
        self.assertEqual(foo.boolean_field, None)
        self.assertEqual(foo.fake_many_to_one, None)

        new_active_session()

        # Test the FooDeepCache.
        foo = Foo(foo_key)
        bar = Bar(bar_key)
        foo.load(FooDeepCache)

        self.assertEqual(type(foo.uuid), UUID)
        self.assertEqual(foo.uuid, foo_key)
        self.assertEqual(foo.str_field, None)
        self.assertEqual(foo.uni_field, None)
        self.assertEqual(foo.int_field, None)
        self.assertEqual(foo.float_field, None)
        # There is a hack on this now.
#        self.assertEqual(foo.enum_field, None)
        self.assertEqual(foo.enum_field, FooEnum[0])
        self.assertEqual(foo.boolean_field, None)
        self.assertEqual(foo.fake_many_to_one, None)

        new_active_session()



#    # This is no longer our specification.  If you migrate a field with a
#    # non-None default you must loop over the existing rows and assign
#    # the default value.
#    def test_migration(self):
#        """Fields should be readable after a migration."""
#        self.reset_cassandra()
#        from util.log import logger
#
#        # Create Persistent object.
#        foo = Foo.create()
#        foo_key = foo.uuid
#        foo.str_field = 'abcd'
#        foo.uni_field = u'abcd'
#        foo.int_field = 22
#        foo.float_field = 3.14159265
#        foo.enum_field = FooEnum[2]
#        foo.boolean_field = True
#
#        save()
#
#        # Simulate a migration of an enum column..
#        stmt = "ALTER TABLE foo_object DROP COLUMN enum_field;"
#        postgresql_execute(stmt)
#        stmt = "ALTER TABLE foo_object ADD COLUMN enum_field INTEGER;"
#        postgresql_execute(stmt)
#        # This has timing issues and won't work in Cassandra.
##        from util.cassandra import add_column, drop_column
##        drop_column('b_foo', 'enum_field')
##        add_column('b_foo', 'enum_field', 'int')
#
#        new_active_session()
#
#        # Test that the Persistent object does as we expect.
#        foo_persist = FooPersist.get_by(uuid=foo_key)
#        self.assertEqual(foo_persist.enum_field, FooEnum[0])
#
#        new_active_session()
#
#        # Test that we can load these values back from MainPersistent.
#        foo = Foo(foo_key)
#        foo.load(MainPersistent)
#
#        self.assertEqual(foo.enum_field, FooEnum[0])
#
#        new_active_session()
#
#        foo = Foo(foo_key)
#        foo.load(FooCache)
#        self.assertEqual(foo.enum_field, FooEnum[0])
#        # Save the FooCache.
#        foo = Foo(foo_key)
#        foo.save(FooCache)
#        save()
#
#        foo = Foo(foo_key)
#        foo.load(FooCache)
#        self.assertEqual(foo.enum_field, FooEnum[0])
#
#        new_active_session()
#
#        # Test Cassandra.
#        # Note, we need to do some fakery here to get our None in cassandra,
#        # because we can't just drop and add the column, Cassandra remembers
#        # the old value.
#        foo_key = backend.core.generate_id()
#        #stmt = "INSERT INTO b_foo (uuid, uni_field) VALUES (uuid:=%s, uni_field:='');" % str(foo_key)
#        stmt = "INSERT INTO b_Foo (uuid, uni_field, hsn_deleted) VALUES (:uuid, :uni_field, 'false')"
#        util.cassandra.batch(cfs_data_pairs={
#        ('b_Foo', ('uuid',)): [
#            {'uuid': foo_key,
#             'uni_field': u'',
#             'hsn_deleted': False}]})
#
#        foo = Foo(foo_key)
#        foo.load(FooLoader)
#        self.assertEqual(foo.enum_field, FooEnum[0])

    def test_count_only_orm_save(self):
        """Count only ORM save."""
        global ACCELERATED_SAVE
        ACCELERATED_SAVE = False
        self._t_count_only()

    def test_count_only_accelerated_save(self):
        """Count only accelerated save."""
        global ACCELERATED_SAVE
        ACCELERATED_SAVE = True
        self._t_count_only()

    def _t_count_only(self):
        """Count only field should work."""
        self.reset_cassandra()
        from util.log import logger

        c = CountOnly.create()
        c_key = c.uuid
        save()

        c_persist = CountOnlyPersist.get_by(uuid=c_key)
        self.assertEqual(c_persist.inc1, 2)
        self.assertEqual(c_persist.inc2, None)

        save()

        c = CountOnly(c_key)
        c.load(MainPersistent)
        self.assertEqual(c.inc1, 2)
        # xxx this is a hack, put it back after we remove hack from environments.persistent and have
        # migrated all increment fields to have a 0 instead of a None.
        self.assertEqual(c.inc2, 0)#None)

        save()

        c = CountOnly(c_key)
        self.assertTrue(c.exists)

        save()

        self.assertTrue(Cassandra.exists(CountOnly, c_key))

    def test_key_only_orm_save(self):
        """Key only ORM save."""
        global ACCELERATED_SAVE
        ACCELERATED_SAVE = False
        self._t_key_only()

    def test_key_only_accelerated_save(self):
        """Key only accelerated save."""
        global ACCELERATED_SAVE
        ACCELERATED_SAVE = True
        self._t_key_only()

    def _t_key_only(self):
        """Key only field should work."""
        self.reset_cassandra()
        from util.log import logger

        k = KeyOnly.create()
        k_key = k.uuid
        save()

        k_persist = KeyOnlyPersist.get_by(uuid=k_key)
        self.assertNotEqual(k_persist, None)

        save()

        k = KeyOnly(k_key)
        k.load(MainPersistent)
        self.assertTrue(k.exists)

        save()

        k = KeyOnly(k_key)
        self.assertTrue(k.exists)

        save()

        self.assertTrue(Cassandra.exists(KeyOnly, k_key))

    def test_exceptions(self):
        """Exceptions should raise under error conditions."""
        str_uuid = str(generate_id())
        # If a key is passed in to .create it must be of the correct type.
        raised = False
        try:
            Foo.create(str_uuid)
        except ValueError:
            raised = True
        self.assertTrue(raised)

        new_active_session()

        # We don't detect errors here because if there are a lot of Entities
        # it gets expensive.
        foo = Foo(str_uuid)
        # However, there tends not to be a lot of assignment, so we can
        # test for the correct key at that point.
        raised = False
        try:
            foo.uni_field = u'unival'
        except ValueError:
            raised = True
        self.assertTrue(raised)
