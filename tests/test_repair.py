
from uuid import UUID

from datetime import datetime, date, time, timedelta
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
import util.database
from util.cache import get_redis
from util.when import to_hsntime, to_timestorage, to_hsndate

db_name = 'test_repair'

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
    cache_list = [FooShallowCache, FooDeepCache]
    loader_list = [
        FooLoader,
        FooByDatetimeLoader,
        FooByDateLoader,
        FooByTimeLoader,
        CountOnlyLoader
    ]

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

    indexes = {
        'bydatetime': ('enum_field', 'datetime_field', 'uuid'),
        'bydate': ('enum_field', 'date_field', 'uuid'),
        'bytime': ('enum_field', 'time_field', 'uuid'),
    }

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
        'inc3_field', 'fake_many_to_one', 'many_to_one', 'datetime_field',
        'date_field', 'time_field'
    ]
    key_template = 'test_general_shallow_cache_%s'

class FooDeepCache(Cache):
    entity_class = Foo
    inventory = FooShallowCache.inventory
    key_template = 'test_general_deep_cache_%s'

class FooLoader(InventoryLoader):
    entity_class = Foo
    inventory = FooShallowCache.inventory

class FooByDatetimeLoader(CompositeKeyLoader):
    entity_class = Foo
    composite_key_name = 'bydatetime'
    inventory = FooShallowCache.inventory

class FooByDateLoader(CompositeKeyLoader):
    entity_class = Foo
    composite_key_name = 'bydate'
    inventory = FooShallowCache.inventory

class FooByTimeLoader(CompositeKeyLoader):
    entity_class = Foo
    composite_key_name = 'bytime'
    inventory = FooShallowCache.inventory

class CountOnlyLoader(InventoryLoader):
    entity_class = CountOnly
    inventory = ['uuid', 'inc1', 'inc2']


def postgresql_execute(stmt):
    from sqlalchemy import create_engine
    import psycopg2
    engine = create_engine(util.database.get_db_url(db_name), echo=False)
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

    def test_count_only(self):
        """Count only repair"""
        self.reset_cassandra()

        entities = [p.entity_class for p in collection]
        from backend.core import repair_cassandra_from_postgres

        # It seems my kludge for testing columns by deleting them may be
        # unreliable.  Let's create a CountOnly where we never write
        # the Cassandra at all, repair it, and see what happens.
        new_active_session()
        co = CountOnly.create()
        co_key = co.uuid
        co.increment('inc1', 11)
        co.increment('inc2', 22)
        get_active_session().save(cassandra=False, try_later_on_this_thread_first=True)
        new_active_session()

        result = repair_cassandra_from_postgres(entity_classes=entities)
        self.assertEqual(result, 2)

        result = repair_cassandra_from_postgres(entity_classes=entities,
                                                repair=True)
        self.assertEqual(result, 0)
        new_active_session()

        co = CountOnly(co_key)
        co.load(CountOnlyLoader)
        self.assertEqual(co.inc1, 13)
        self.assertEqual(co.inc2, 22)

        result = repair_cassandra_from_postgres(entity_classes=entities)
        self.assertEqual(result, 0)

        result = repair_cassandra_from_postgres(
            entity_classes=entities,
            repair=True,
            verify=False)
        self.assertEqual(result, 0)

        result = repair_cassandra_from_postgres(
            entity_classes=entities,
            overwrite=True,
            verify=False)
        self.assertEqual(result, 2)

    def test_repair(self):
        """Repair"""
        self.reset_cassandra()

        entities = [p.entity_class for p in collection]
        from backend.core import repair_cassandra_from_postgres

        new_active_session()

        # Create a set of records.
        foo1 = Foo.create()
        foo1_key = foo1.uuid
        foo1.str_field = 'foo1'
        foo1.uni_field = u'foo1\xe5'
        foo1.int_field = 1
        foo1.float_field = 1.1
        foo1.enum_field = FooEnum[0]
        foo1.boolean_field = True
        foo1_datetime = datetime(2001, 5, 17, 22, 58, 59)
        foo1.datetime_field = foo1_datetime
        foo1_date = date(2002, 10, 3)
        foo1.date_field = foo1_date
        foo1_time = time(2, 30)
        foo1.time_field = foo1_time
        foo1.increment('inc1_field', 11)
        foo1.increment('inc2_field', 12)
        foo1.increment('inc3_field', 13)

        foo2 = Foo.create()
        foo2_key = foo2.uuid
        foo2.str_field = 'foo2'
        foo2.uni_field = u'foo2\xe5'
        foo2.int_field = 2
        foo2.float_field = 2.2
        foo2.enum_field = FooEnum[1]
        foo2.boolean_field = True
        foo2_datetime = datetime(1997, 5, 17, 22, 58, 59)
        foo2.datetime_field = foo2_datetime
        foo2_date = date(1998, 10, 3)
        foo2.date_field = foo2_date
        foo2_time = time(5, 30)
        foo2.time_field = foo2_time
        foo2.increment('inc1_field', 21)
        foo2.increment('inc2_field', 22)
        foo2.increment('inc3_field', 23)

        foo3 = Foo.create()
        foo3_key = foo3.uuid
        foo3.str_field = 'foo3'
        foo3.uni_field = u'foo3\xe5'
        foo3.int_field = 3
        foo3.float_field = 3.3
        foo3.enum_field = FooEnum[2]
        foo3.boolean_field = True
        foo3.increment('inc1_field', 31)
        foo3.increment('inc2_field', 32)
        foo3.increment('inc3_field', 33)

        foo4 = Foo.create()
        foo4_key = foo4.uuid
        foo4.str_field = 'foo4'
        foo4.uni_field = u'foo4\xe5'
        foo4.int_field = 4
        foo4.float_field = 4.4
        foo4.enum_field = FooEnum[0]
        foo4.boolean_field = True
        foo4.increment('inc1_field', 41)
        foo4.increment('inc2_field', 42)
        foo4.increment('inc3_field', 43)

        get_active_session().save(cassandra=False, try_later_on_this_thread_first=True)
        new_active_session()

        foo1 = Foo(foo1_key)
#        foo1.load(FooLoader)
        foo2 = Foo(foo2_key)
#        foo2.load(FooLoader)
        foo3 = Foo(foo3_key)
#        foo3.load(FooLoader)
        foo4 = Foo(foo4_key)
#        foo4.load(FooLoader)

        bar1 = Bar.create()
        bar1_key = bar1.uuid
        bar2 = Bar.create()
        bar2_key = bar2.uuid
        bar3 = Bar.create()
        bar3_key = bar3.uuid
        bar4 = Bar.create()
        bar4_key = bar4.uuid

        get_active_session().save(cassandra=False, try_later_on_this_thread_first=True)
        new_active_session()

        foo1 = FooPersist.get_by(uuid=foo1_key)
        foo2 = FooPersist.get_by(uuid=foo2_key)
        foo3 = FooPersist.get_by(uuid=foo3_key)
        foo4 = FooPersist.get_by(uuid=foo4_key)
        bar1 = BarPersist.get_by(uuid=bar1_key)
        bar2 = BarPersist.get_by(uuid=bar2_key)
        bar3 = BarPersist.get_by(uuid=bar3_key)
        bar4 = BarPersist.get_by(uuid=bar4_key)
        foo1.many_to_one = bar1
        foo2.many_to_one = bar1
        foo1.fake_many_to_one = bar1
        foo3.many_to_one = bar2
        foo4.many_to_one = bar2
        foo2.fake_many_to_one = bar2
        foo3.fake_many_to_one = bar3
        foo4.fake_many_to_one = bar4

        import backend.core
        backend.core.commit_session()
        backend.core.remove_session()
        new_active_session()

        count1 = CountOnly.create()
        count1_key = count1.uuid
        count1.increment('inc1', 11)
        count1.increment('inc2', 12)

        count2 = CountOnly.create()
        count2_key = count2.uuid
        count2.increment('inc1', 21)
        count2.increment('inc2', 22)

        count3 = CountOnly.create()
        count3_key = count3.uuid
        count3.increment('inc1', 31)
        count3.increment('inc2', 32)

        count4 = CountOnly.create()
        count4_key = count4.uuid
        count4.increment('inc1', 41)
        count4.increment('inc2', 42)

        key1 = KeyOnly.create()
        key1_key = key1.uuid

        key2 = KeyOnly.create()
        key2_key = key2.uuid

        key3 = KeyOnly.create()
        key3_key = key3.uuid

        key4 = KeyOnly.create()
        key4_key = key4.uuid

        get_active_session().save(cassandra=False, try_later_on_this_thread_first=True)
        new_active_session()

        entities = [p.entity_class for p in collection]
        result = repair_cassandra_from_postgres(
            entity_classes=entities,
            repair=True)
        self.assertEqual(result, 0)

        def check_original_conditions(check_counters=True):
            # --Foo1-----------------------------------------------------------
            new_active_session()
            foo1 = Foo(foo1_key)
            bar1 = Bar(bar1_key)
            foo1.load(FooLoader)
            self.assertEqual(foo1.str_field, 'foo1')
            self.assertEqual(foo1.uni_field, u'foo1\xe5')
            self.assertEqual(foo1.int_field, 1)
            self.assertEqual(foo1.float_field, 1.1)
            self.assertEqual(foo1.enum_field, FooEnum[0])
            self.assertEqual(foo1.boolean_field, True)
            self.assertEqual(foo1.datetime_field, foo1_datetime)
            self.assertEqual(foo1.date_field, foo1_date)
            self.assertEqual(foo1.time_field, foo1_time)
            if check_counters:
                self.assertEqual(foo1.inc1_field, 11)
                self.assertEqual(foo1.inc2_field, 14)
                self.assertEqual(foo1.inc3_field, 13)
            self.assertEqual(foo1.many_to_one, bar1)
            self.assertEqual(foo1.fake_many_to_one, bar1)

            new_active_session()
            foo1 = Foo(foo1_key)
            loader = FooByDatetimeLoader([FooEnum[0], foo1_datetime])
            loader.load()
            self.assertEqual(foo1.str_field, 'foo1')
            self.assertEqual(foo1.uni_field, u'foo1\xe5')
            self.assertEqual(foo1.int_field, 1)
            self.assertEqual(foo1.float_field, 1.1)
            self.assertEqual(foo1.enum_field, FooEnum[0])
            self.assertEqual(foo1.boolean_field, True)
            self.assertEqual(foo1.datetime_field, foo1_datetime)
            self.assertEqual(foo1.date_field, foo1_date)
            self.assertEqual(foo1.time_field, foo1_time)
            if check_counters:
                self.assertEqual(foo1.inc1_field, 11)
                self.assertEqual(foo1.inc2_field, 14)
                self.assertEqual(foo1.inc3_field, 13)
            self.assertEqual(foo1.many_to_one, bar1)
            self.assertEqual(foo1.fake_many_to_one, bar1)

            new_active_session()
            foo1 = Foo(foo1_key)
            loader = FooByDateLoader([FooEnum[0], foo1_date])
            loader.load()
            self.assertEqual(foo1.str_field, 'foo1')
            self.assertEqual(foo1.uni_field, u'foo1\xe5')
            self.assertEqual(foo1.int_field, 1)
            self.assertEqual(foo1.float_field, 1.1)
            self.assertEqual(foo1.enum_field, FooEnum[0])
            self.assertEqual(foo1.boolean_field, True)
            self.assertEqual(foo1.datetime_field, foo1_datetime)
            self.assertEqual(foo1.date_field, foo1_date)
            self.assertEqual(foo1.time_field, foo1_time)
            if check_counters:
                self.assertEqual(foo1.inc1_field, 11)
                self.assertEqual(foo1.inc2_field, 14)
                self.assertEqual(foo1.inc3_field, 13)
            self.assertEqual(foo1.many_to_one, bar1)
            self.assertEqual(foo1.fake_many_to_one, bar1)

            new_active_session()
            foo1 = Foo(foo1_key)
            loader = FooByTimeLoader([FooEnum[0], foo1_time])
            loader.load()
            self.assertEqual(foo1.str_field, 'foo1')
            self.assertEqual(foo1.uni_field, u'foo1\xe5')
            self.assertEqual(foo1.int_field, 1)
            self.assertEqual(foo1.float_field, 1.1)
            self.assertEqual(foo1.enum_field, FooEnum[0])
            self.assertEqual(foo1.boolean_field, True)
            self.assertEqual(foo1.datetime_field, foo1_datetime)
            self.assertEqual(foo1.date_field, foo1_date)
            self.assertEqual(foo1.time_field, foo1_time)
            if check_counters:
                self.assertEqual(foo1.inc1_field, 11)
                self.assertEqual(foo1.inc2_field, 14)
                self.assertEqual(foo1.inc3_field, 13)
            self.assertEqual(foo1.many_to_one, bar1)
            self.assertEqual(foo1.fake_many_to_one, bar1)

            # --Foo2---------------------------------------------------------------
            new_active_session()
            foo2 = Foo(foo2_key)
            bar1 = Bar(bar1_key)
            bar2 = Bar(bar2_key)
            foo2.load(FooLoader)
            self.assertEqual(foo2.str_field, 'foo2')
            self.assertEqual(foo2.uni_field, u'foo2\xe5')
            self.assertEqual(foo2.int_field, 2)
            self.assertEqual(foo2.float_field, 2.2)
            self.assertEqual(foo2.enum_field, FooEnum[1])
            self.assertEqual(foo2.boolean_field, True)
            self.assertEqual(foo2.datetime_field, foo2_datetime)
            self.assertEqual(foo2.date_field, foo2_date)
            self.assertEqual(foo2.time_field, foo2_time)
            if check_counters:
                self.assertEqual(foo2.inc1_field, 21)
                self.assertEqual(foo2.inc2_field, 24)
                self.assertEqual(foo2.inc3_field, 23)
            self.assertEqual(foo2.many_to_one, bar1)
            self.assertEqual(foo2.fake_many_to_one, bar2)

            new_active_session()
            foo2 = Foo(foo2_key)
            loader = FooByDatetimeLoader([FooEnum[1], foo2_datetime])
            loader.load()
            self.assertEqual(foo2.str_field, 'foo2')
            self.assertEqual(foo2.uni_field, u'foo2\xe5')
            self.assertEqual(foo2.int_field, 2)
            self.assertEqual(foo2.float_field, 2.2)
            self.assertEqual(foo2.enum_field, FooEnum[1])
            self.assertEqual(foo2.boolean_field, True)
            self.assertEqual(foo2.datetime_field, foo2_datetime)
            self.assertEqual(foo2.date_field, foo2_date)
            self.assertEqual(foo2.time_field, foo2_time)
            if check_counters:
                self.assertEqual(foo2.inc1_field, 21)
                self.assertEqual(foo2.inc2_field, 24)
                self.assertEqual(foo2.inc3_field, 23)
            self.assertEqual(foo2.many_to_one, bar1)
            self.assertEqual(foo2.fake_many_to_one, bar2)

            new_active_session()
            foo2 = Foo(foo2_key)
            loader = FooByDateLoader([FooEnum[1], foo2_date])
            loader.load()
            self.assertEqual(foo2.str_field, 'foo2')
            self.assertEqual(foo2.uni_field, u'foo2\xe5')
            self.assertEqual(foo2.int_field, 2)
            self.assertEqual(foo2.float_field, 2.2)
            self.assertEqual(foo2.enum_field, FooEnum[1])
            self.assertEqual(foo2.boolean_field, True)
            self.assertEqual(foo2.datetime_field, foo2_datetime)
            self.assertEqual(foo2.date_field, foo2_date)
            self.assertEqual(foo2.time_field, foo2_time)
            if check_counters:
                self.assertEqual(foo2.inc1_field, 21)
                self.assertEqual(foo2.inc2_field, 24)
                self.assertEqual(foo2.inc3_field, 23)
            self.assertEqual(foo2.many_to_one, bar1)
            self.assertEqual(foo2.fake_many_to_one, bar2)

            new_active_session()
            foo2 = Foo(foo2_key)
            loader = FooByTimeLoader([FooEnum[1], foo2_time])
            loader.load()
            self.assertEqual(foo2.str_field, 'foo2')
            self.assertEqual(foo2.uni_field, u'foo2\xe5')
            self.assertEqual(foo2.int_field, 2)
            self.assertEqual(foo2.float_field, 2.2)
            self.assertEqual(foo2.enum_field, FooEnum[1])
            self.assertEqual(foo2.boolean_field, True)
            self.assertEqual(foo2.datetime_field, foo2_datetime)
            self.assertEqual(foo2.date_field, foo2_date)
            self.assertEqual(foo2.time_field, foo2_time)
            if check_counters:
                self.assertEqual(foo2.inc1_field, 21)
                self.assertEqual(foo2.inc2_field, 24)
                self.assertEqual(foo2.inc3_field, 23)
            self.assertEqual(foo2.many_to_one, bar1)
            self.assertEqual(foo2.fake_many_to_one, bar2)

            # --Foo3---------------------------------------------------------------
            new_active_session()
            foo3 = Foo(foo3_key)
            bar2 = Bar(bar2_key)
            bar3 = Bar(bar3_key)
            foo3.load(FooLoader)
            self.assertEqual(foo3.str_field, 'foo3')
            self.assertEqual(foo3.uni_field, u'foo3\xe5')
            self.assertEqual(foo3.int_field, 3)
            self.assertEqual(foo3.float_field, 3.3)
            self.assertEqual(foo3.enum_field, FooEnum[2])
            self.assertEqual(foo3.boolean_field, True)
            self.assertEqual(foo3.datetime_field, None)
            self.assertEqual(foo3.date_field, None)
            self.assertEqual(foo3.time_field, None)
            if check_counters:
                self.assertEqual(foo3.inc1_field, 31)
                self.assertEqual(foo3.inc2_field, 34)
                self.assertEqual(foo3.inc3_field, 33)
            self.assertEqual(foo3.many_to_one, bar2)
            self.assertEqual(foo3.fake_many_to_one, bar3)

            # --Foo4---------------------------------------------------------------
            new_active_session()
            foo4 = Foo(foo4_key)
            bar2 = Bar(bar2_key)
            bar4 = Bar(bar4_key)
            foo4.load(FooLoader)
            self.assertEqual(foo4.str_field, 'foo4')
            self.assertEqual(foo4.uni_field, u'foo4\xe5')
            self.assertEqual(foo4.int_field, 4)
            self.assertEqual(foo4.float_field, 4.4)
            self.assertEqual(foo4.enum_field, FooEnum[0])
            self.assertEqual(foo4.boolean_field, True)
            self.assertEqual(foo4.datetime_field, None)
            self.assertEqual(foo4.date_field, None)
            self.assertEqual(foo4.time_field, None)
            if check_counters:
                self.assertEqual(foo4.inc1_field, 41)
                self.assertEqual(foo4.inc2_field, 44)
                self.assertEqual(foo4.inc3_field, 43)
            self.assertEqual(foo4.many_to_one, bar2)
            self.assertEqual(foo4.fake_many_to_one, bar4)

        check_original_conditions()

        result = repair_cassandra_from_postgres(entity_classes=entities)
        self.assertEqual(result, 0)

        # The make a few common discrepencies in Cassandra.
        # foo1 is normal
        # foo2 is missing two of three indexs and one incr field, and a scalar error.
        # foo3 is missing its incr CF and a few scalar fields.
        # foo4 does not exist at all.

        def prepare_cassandra():
            # foo2 is missing two of three indexs and one incr field, and a scalar error.
            util.cassandra.batch(deletes={
                'b_foo_bydatetime': [{'uuid': foo2_key, 'datetime_field': to_hsntime(foo2_datetime), 'enum_field': 1}],
                'b_foo_bytime': [{'uuid': foo2_key, 'time_field': to_timestorage(foo2_time), 'enum_field': 1}]
            })
            # I don't know how to delete a counter column so let's delete the CF
            # and remake it.
            # NOTE: this behavior is undefined in Cassandra.
            util.cassandra.batch(deletes={
                'b_foo__counts': [{'uuid': foo2_key}]
            })
            util.cassandra.batch(increments={
                ('b_foo__counts', ('uuid',),): [{'uuid': foo2_key, 'inc1_field': 21, 'inc3_field': 23}]
            })
            util.cassandra.batch(cfs_data_pairs={
                ('b_foo', ('uuid',),): [{'uuid': foo2_key, 'uni_field': u'fooxxx\xe5'}]
            })

            # foo3 is missing its incr CF and a few scalar fields.
            util.cassandra.batch(deletes={
                'b_foo__counts': [{'uuid': foo3_key}]
            })
            util.cassandra.batch(cfs_data_pairs={
                ('b_foo', ('uuid',),): [{'uuid': foo3_key,
                                         'uni_field': None,
                                         'float_field': None}]
            })

            # foo4 does not exist at all.
            util.cassandra.batch(deletes={
                'b_foo__counts': [{'uuid': foo4_key}],
                'b_foo': [{'uuid': foo4_key}]
            })

        prepare_cassandra()

        # --Foo1---------------------------------------------------------------
        new_active_session()
        foo1 = Foo(foo1_key)
        bar1 = Bar(bar1_key)
        foo1.load(FooLoader)
        self.assertEqual(foo1.str_field, 'foo1')
        self.assertEqual(foo1.uni_field, u'foo1\xe5')
        self.assertEqual(foo1.int_field, 1)
        self.assertEqual(foo1.float_field, 1.1)
        self.assertEqual(foo1.enum_field, FooEnum[0])
        self.assertEqual(foo1.boolean_field, True)
        self.assertEqual(foo1.datetime_field, foo1_datetime)
        self.assertEqual(foo1.date_field, foo1_date)
        self.assertEqual(foo1.time_field, foo1_time)
        self.assertEqual(foo1.inc1_field, 11)
        self.assertEqual(foo1.inc2_field, 14)
        self.assertEqual(foo1.inc3_field, 13)
        self.assertEqual(foo1.many_to_one, bar1)
        self.assertEqual(foo1.fake_many_to_one, bar1)

        new_active_session()
        foo1 = Foo(foo1_key)
        loader = FooByDatetimeLoader([FooEnum[0], foo1_datetime])
        loader.load()
        self.assertEqual(foo1.str_field, 'foo1')
        self.assertEqual(foo1.uni_field, u'foo1\xe5')
        self.assertEqual(foo1.int_field, 1)
        self.assertEqual(foo1.float_field, 1.1)
        self.assertEqual(foo1.enum_field, FooEnum[0])
        self.assertEqual(foo1.boolean_field, True)
        self.assertEqual(foo1.datetime_field, foo1_datetime)
        self.assertEqual(foo1.date_field, foo1_date)
        self.assertEqual(foo1.time_field, foo1_time)
        self.assertEqual(foo1.inc1_field, 11)
        self.assertEqual(foo1.inc2_field, 14)
        self.assertEqual(foo1.inc3_field, 13)
        self.assertEqual(foo1.many_to_one, bar1)
        self.assertEqual(foo1.fake_many_to_one, bar1)

        new_active_session()
        foo1 = Foo(foo1_key)
        loader = FooByDateLoader([FooEnum[0], foo1_date])
        loader.load()
        self.assertEqual(foo1.str_field, 'foo1')
        self.assertEqual(foo1.uni_field, u'foo1\xe5')
        self.assertEqual(foo1.int_field, 1)
        self.assertEqual(foo1.float_field, 1.1)
        self.assertEqual(foo1.enum_field, FooEnum[0])
        self.assertEqual(foo1.boolean_field, True)
        self.assertEqual(foo1.datetime_field, foo1_datetime)
        self.assertEqual(foo1.date_field, foo1_date)
        self.assertEqual(foo1.time_field, foo1_time)
        self.assertEqual(foo1.inc1_field, 11)
        self.assertEqual(foo1.inc2_field, 14)
        self.assertEqual(foo1.inc3_field, 13)
        self.assertEqual(foo1.many_to_one, bar1)
        self.assertEqual(foo1.fake_many_to_one, bar1)

        new_active_session()
        foo1 = Foo(foo1_key)
        loader = FooByTimeLoader([FooEnum[0], foo1_time])
        loader.load()
        self.assertEqual(foo1.str_field, 'foo1')
        self.assertEqual(foo1.uni_field, u'foo1\xe5')
        self.assertEqual(foo1.int_field, 1)
        self.assertEqual(foo1.float_field, 1.1)
        self.assertEqual(foo1.enum_field, FooEnum[0])
        self.assertEqual(foo1.boolean_field, True)
        self.assertEqual(foo1.datetime_field, foo1_datetime)
        self.assertEqual(foo1.date_field, foo1_date)
        self.assertEqual(foo1.time_field, foo1_time)
        self.assertEqual(foo1.inc1_field, 11)
        self.assertEqual(foo1.inc2_field, 14)
        self.assertEqual(foo1.inc3_field, 13)
        self.assertEqual(foo1.many_to_one, bar1)
        self.assertEqual(foo1.fake_many_to_one, bar1)

        # --Foo2---------------------------------------------------------------
        new_active_session()
        foo2 = Foo(foo2_key)
        bar1 = Bar(bar1_key)
        bar2 = Bar(bar2_key)
        foo2.load(FooLoader)
        self.assertEqual(foo2.str_field, 'foo2')
        self.assertEqual(foo2.uni_field, u'fooxxx\xe5')
        self.assertEqual(foo2.int_field, 2)
        self.assertEqual(foo2.float_field, 2.2)
        self.assertEqual(foo2.enum_field, FooEnum[1])
        self.assertEqual(foo2.boolean_field, True)
        self.assertEqual(foo2.datetime_field, foo2_datetime)
        self.assertEqual(foo2.date_field, foo2_date)
        self.assertEqual(foo2.time_field, foo2_time)
        self.assertEqual(foo2.inc1_field, 0)
        self.assertEqual(foo2.inc2_field, 0)
        self.assertEqual(foo2.inc3_field, 0)
        self.assertEqual(foo2.many_to_one, bar1)
        self.assertEqual(foo2.fake_many_to_one, bar2)

        new_active_session()
        foo2 = Foo(foo2_key)
        loader = FooByDatetimeLoader([FooEnum[1], foo2_datetime])
        loader.load()
        self.assertEqual(len(loader.result), 0)

        new_active_session()
        foo2 = Foo(foo2_key)
        loader = FooByDateLoader([FooEnum[1], foo2_date])
        loader.load()
        self.assertEqual(foo2.str_field, 'foo2')
        self.assertEqual(foo2.uni_field, u'foo2\xe5')
        self.assertEqual(foo2.int_field, 2)
        self.assertEqual(foo2.float_field, 2.2)
        self.assertEqual(foo2.enum_field, FooEnum[1])
        self.assertEqual(foo2.boolean_field, True)
        self.assertEqual(foo2.datetime_field, foo2_datetime)
        self.assertEqual(foo2.date_field, foo2_date)
        self.assertEqual(foo2.time_field, foo2_time)
        self.assertEqual(foo2.inc1_field, 0)
        self.assertEqual(foo2.inc2_field, 0)
        self.assertEqual(foo2.inc3_field, 0)
        self.assertEqual(foo2.many_to_one, bar1)
        self.assertEqual(foo2.fake_many_to_one, bar2)

        new_active_session()
        foo2 = Foo(foo2_key)
        loader = FooByTimeLoader([FooEnum[1], foo2_time])
        loader.load()
        self.assertEqual(len(loader.result), 0)

        # --Foo3---------------------------------------------------------------
        new_active_session()
        foo3 = Foo(foo3_key)
        bar2 = Bar(bar2_key)
        bar3 = Bar(bar3_key)
        foo3.load(FooLoader)
        self.assertEqual(foo3.str_field, 'foo3')
        self.assertEqual(foo3.uni_field, None)
        self.assertEqual(foo3.int_field, 3)
        self.assertEqual(foo3.float_field, None)
        self.assertEqual(foo3.enum_field, FooEnum[2])
        self.assertEqual(foo3.boolean_field, True)
        self.assertEqual(foo3.datetime_field, None)
        self.assertEqual(foo3.date_field, None)
        self.assertEqual(foo3.time_field, None)
        self.assertEqual(foo3.inc1_field, 0)
        self.assertEqual(foo3.inc2_field, 0)
        self.assertEqual(foo3.inc3_field, 0)
        self.assertEqual(foo3.many_to_one, bar2)
        self.assertEqual(foo3.fake_many_to_one, bar3)

        # --Foo4---------------------------------------------------------------
        new_active_session()
        foo4 = Foo(foo4_key)
        raised = False
        try:
            foo4.load(FooLoader)
        except NullEnvironmentError:
            raised =True
        self.assertEqual(raised, True)

        count = repair_cassandra_from_postgres(entity_classes=entities)
        self.assertTrue(count > 0)

        # Confirm that the repair did not alter anything.
        # (repair=False by default.)

        def confirm_repair_did_nothing():
            # --Foo1---------------------------------------------------------------
            new_active_session()
            foo1 = Foo(foo1_key)
            bar1 = Bar(bar1_key)
            foo1.load(FooLoader)
            self.assertEqual(foo1.str_field, 'foo1')
            self.assertEqual(foo1.uni_field, u'foo1\xe5')
            self.assertEqual(foo1.int_field, 1)
            self.assertEqual(foo1.float_field, 1.1)
            self.assertEqual(foo1.enum_field, FooEnum[0])
            self.assertEqual(foo1.boolean_field, True)
            self.assertEqual(foo1.datetime_field, foo1_datetime)
            self.assertEqual(foo1.date_field, foo1_date)
            self.assertEqual(foo1.time_field, foo1_time)
            self.assertEqual(foo1.inc1_field, 11)
            self.assertEqual(foo1.inc2_field, 14)
            self.assertEqual(foo1.inc3_field, 13)
            self.assertEqual(foo1.many_to_one, bar1)
            self.assertEqual(foo1.fake_many_to_one, bar1)

            new_active_session()
            foo1 = Foo(foo1_key)
            loader = FooByDatetimeLoader([FooEnum[0], foo1_datetime])
            loader.load()
            self.assertEqual(foo1.str_field, 'foo1')
            self.assertEqual(foo1.uni_field, u'foo1\xe5')
            self.assertEqual(foo1.int_field, 1)
            self.assertEqual(foo1.float_field, 1.1)
            self.assertEqual(foo1.enum_field, FooEnum[0])
            self.assertEqual(foo1.boolean_field, True)
            self.assertEqual(foo1.datetime_field, foo1_datetime)
            self.assertEqual(foo1.date_field, foo1_date)
            self.assertEqual(foo1.time_field, foo1_time)
            self.assertEqual(foo1.inc1_field, 11)
            self.assertEqual(foo1.inc2_field, 14)
            self.assertEqual(foo1.inc3_field, 13)
            self.assertEqual(foo1.many_to_one, bar1)
            self.assertEqual(foo1.fake_many_to_one, bar1)

            new_active_session()
            foo1 = Foo(foo1_key)
            loader = FooByDateLoader([FooEnum[0], foo1_date])
            loader.load()
            self.assertEqual(foo1.str_field, 'foo1')
            self.assertEqual(foo1.uni_field, u'foo1\xe5')
            self.assertEqual(foo1.int_field, 1)
            self.assertEqual(foo1.float_field, 1.1)
            self.assertEqual(foo1.enum_field, FooEnum[0])
            self.assertEqual(foo1.boolean_field, True)
            self.assertEqual(foo1.datetime_field, foo1_datetime)
            self.assertEqual(foo1.date_field, foo1_date)
            self.assertEqual(foo1.time_field, foo1_time)
            self.assertEqual(foo1.inc1_field, 11)
            self.assertEqual(foo1.inc2_field, 14)
            self.assertEqual(foo1.inc3_field, 13)
            self.assertEqual(foo1.many_to_one, bar1)
            self.assertEqual(foo1.fake_many_to_one, bar1)

            new_active_session()
            foo1 = Foo(foo1_key)
            loader = FooByTimeLoader([FooEnum[0], foo1_time])
            loader.load()
            self.assertEqual(foo1.str_field, 'foo1')
            self.assertEqual(foo1.uni_field, u'foo1\xe5')
            self.assertEqual(foo1.int_field, 1)
            self.assertEqual(foo1.float_field, 1.1)
            self.assertEqual(foo1.enum_field, FooEnum[0])
            self.assertEqual(foo1.boolean_field, True)
            self.assertEqual(foo1.datetime_field, foo1_datetime)
            self.assertEqual(foo1.date_field, foo1_date)
            self.assertEqual(foo1.time_field, foo1_time)
            self.assertEqual(foo1.inc1_field, 11)
            self.assertEqual(foo1.inc2_field, 14)
            self.assertEqual(foo1.inc3_field, 13)
            self.assertEqual(foo1.many_to_one, bar1)
            self.assertEqual(foo1.fake_many_to_one, bar1)

            # --Foo2---------------------------------------------------------------
            new_active_session()
            foo2 = Foo(foo2_key)
            bar1 = Bar(bar1_key)
            bar2 = Bar(bar2_key)
            foo2.load(FooLoader)
            self.assertEqual(foo2.str_field, 'foo2')
            self.assertEqual(foo2.uni_field, u'fooxxx\xe5')
            self.assertEqual(foo2.int_field, 2)
            self.assertEqual(foo2.float_field, 2.2)
            self.assertEqual(foo2.enum_field, FooEnum[1])
            self.assertEqual(foo2.boolean_field, True)
            self.assertEqual(foo2.datetime_field, foo2_datetime)
            self.assertEqual(foo2.date_field, foo2_date)
            self.assertEqual(foo2.time_field, foo2_time)
            self.assertEqual(foo2.inc1_field, 0)
            self.assertEqual(foo2.inc2_field, 0)
            self.assertEqual(foo2.inc3_field, 0)
            self.assertEqual(foo2.many_to_one, bar1)
            self.assertEqual(foo2.fake_many_to_one, bar2)

            new_active_session()
            foo2 = Foo(foo2_key)
            loader = FooByDatetimeLoader([FooEnum[1], foo2_datetime])
            loader.load()
            self.assertEqual(len(loader.result), 0)

            new_active_session()
            foo2 = Foo(foo2_key)
            loader = FooByDateLoader([FooEnum[1], foo2_date])
            loader.load()
            self.assertEqual(foo2.str_field, 'foo2')
            self.assertEqual(foo2.uni_field, u'foo2\xe5')
            self.assertEqual(foo2.int_field, 2)
            self.assertEqual(foo2.float_field, 2.2)
            self.assertEqual(foo2.enum_field, FooEnum[1])
            self.assertEqual(foo2.boolean_field, True)
            self.assertEqual(foo2.datetime_field, foo2_datetime)
            self.assertEqual(foo2.date_field, foo2_date)
            self.assertEqual(foo2.time_field, foo2_time)
            self.assertEqual(foo2.inc1_field, 0)
            self.assertEqual(foo2.inc2_field, 0)
            self.assertEqual(foo2.inc3_field, 0)
            self.assertEqual(foo2.many_to_one, bar1)
            self.assertEqual(foo2.fake_many_to_one, bar2)

            new_active_session()
            foo2 = Foo(foo2_key)
            loader = FooByTimeLoader([FooEnum[1], foo2_time])
            loader.load()
            self.assertEqual(len(loader.result), 0)

            # --Foo3---------------------------------------------------------------
            new_active_session()
            foo3 = Foo(foo3_key)
            bar2 = Bar(bar2_key)
            bar3 = Bar(bar3_key)
            foo3.load(FooLoader)
            self.assertEqual(foo3.str_field, 'foo3')
            self.assertEqual(foo3.uni_field, None)
            self.assertEqual(foo3.int_field, 3)
            self.assertEqual(foo3.float_field, None)
            self.assertEqual(foo3.enum_field, FooEnum[2])
            self.assertEqual(foo3.boolean_field, True)
            self.assertEqual(foo3.datetime_field, None)
            self.assertEqual(foo3.date_field, None)
            self.assertEqual(foo3.time_field, None)
            self.assertEqual(foo3.inc1_field, 0)
            self.assertEqual(foo3.inc2_field, 0)
            self.assertEqual(foo3.inc3_field, 0)
            self.assertEqual(foo3.many_to_one, bar2)
            self.assertEqual(foo3.fake_many_to_one, bar3)

            # --Foo4---------------------------------------------------------------
            new_active_session()
            foo4 = Foo(foo4_key)
            raised = False
            try:
                foo4.load(FooLoader)
            except NullEnvironmentError:
                raised =True
            self.assertEqual(raised, True)

        confirm_repair_did_nothing()

        new_active_session()

        # Let's test 'skip'.
        result = repair_cassandra_from_postgres(
            skip='Foo',
            entity_classes=entities,
            is_alive_timeout=timedelta(milliseconds=1),
            repair=True)
        self.assertEqual(result, 0)

        confirm_repair_did_nothing()

        result = repair_cassandra_from_postgres(
            skip=['Foo', 'KeyOnly'],
            entity_classes=entities,
            repair=True)
        self.assertEqual(result, 0)

        confirm_repair_did_nothing()

        result = repair_cassandra_from_postgres(
            skip=Foo,
            entity_classes=entities,
            repair=True)
        self.assertEqual(result, 0)

        confirm_repair_did_nothing()

        result = repair_cassandra_from_postgres(
            skip=[Foo, 'KeyOnly'],
            entity_classes=entities,
            repair=True)
        self.assertEqual(result, 0)

        confirm_repair_did_nothing()

        # Let's do the repair.
        result = repair_cassandra_from_postgres(
            entity_classes=entities,
            repair=True)
        self.assertEqual(result, 0)

        check_original_conditions(check_counters=False)

    def test_overwrite(self):
        """Overwrite"""
        self.reset_cassandra()

        entities = [p.entity_class for p in collection]
        from backend.core import repair_cassandra_from_postgres

        # Create a set of records.
        foo1 = Foo.create()
        foo1_key = foo1.uuid
        foo1.str_field = 'foo1'
        foo1.uni_field = u'foo1\xe5'
        foo1.int_field = 1
        foo1.float_field = 1.1
        foo1.enum_field = FooEnum[0]
        foo1.boolean_field = True
        foo1_datetime = datetime(2001, 5, 17, 22, 58, 59)
        foo1.datetime_field = foo1_datetime
        foo1_date = date(2002, 10, 3)
        foo1.date_field = foo1_date
        foo1_time = time(2, 30)
        foo1.time_field = foo1_time
        foo1.increment('inc1_field', 11)
        foo1.increment('inc2_field', 12)
        foo1.increment('inc3_field', 13)

        foo2 = Foo.create()
        foo2_key = foo2.uuid
        foo2.str_field = 'foo2'
        foo2.uni_field = u'foo2\xe5'
        foo2.int_field = 2
        foo2.float_field = 2.2
        foo2.enum_field = FooEnum[1]
        foo2.boolean_field = True
        foo2_datetime = datetime(2005, 5, 17, 22, 58, 59)
        foo2.datetime_field = foo2_datetime
        foo2_date = date(1999, 10, 3)
        foo2.date_field = foo2_date
        foo2_time = time(4, 30)
        foo2.time_field = foo2_time
        foo2.increment('inc1_field', 21)
        foo2.increment('inc2_field', 22)
        foo2.increment('inc3_field', 23)

        foo3 = Foo.create()
        foo3_key = foo3.uuid
        foo3.str_field = 'foo3'
        foo3.uni_field = u'foo3\xe5'
        foo3.int_field = 3
        foo3.float_field = 3.3
        foo3.enum_field = FooEnum[2]
        foo3.boolean_field = True
        foo3.increment('inc1_field', 31)
        foo3.increment('inc2_field', 32)
        foo3.increment('inc3_field', 33)

        foo4 = Foo.create()
        foo4_key = foo4.uuid
        foo4.str_field = 'foo4'
        foo4.uni_field = u'foo4\xe5'
        foo4.int_field = 4
        foo4.float_field = 4.4
        foo4.enum_field = FooEnum[0]
        foo4.boolean_field = True
        foo4.increment('inc1_field', 41)
        foo4.increment('inc2_field', 42)
        foo4.increment('inc3_field', 43)

        get_active_session().save(cassandra=False, try_later_on_this_thread_first=True)
        new_active_session()

        foo1 = Foo(foo1_key)
#        foo1.load(FooLoader)
        foo2 = Foo(foo2_key)
#        foo2.load(FooLoader)
        foo3 = Foo(foo3_key)
#        foo3.load(FooLoader)
        foo4 = Foo(foo4_key)
#        foo4.load(FooLoader)

        bar1 = Bar.create()
        bar1_key = bar1.uuid
        bar2 = Bar.create()
        bar2_key = bar2.uuid
        bar3 = Bar.create()
        bar3_key = bar3.uuid
        bar4 = Bar.create()
        bar4_key = bar4.uuid

        get_active_session().save(cassandra=False, try_later_on_this_thread_first=True)
        new_active_session()

        foo1 = FooPersist.get_by(uuid=foo1_key)
        foo2 = FooPersist.get_by(uuid=foo2_key)
        foo3 = FooPersist.get_by(uuid=foo3_key)
        foo4 = FooPersist.get_by(uuid=foo4_key)
        bar1 = BarPersist.get_by(uuid=bar1_key)
        bar2 = BarPersist.get_by(uuid=bar2_key)
        bar3 = BarPersist.get_by(uuid=bar3_key)
        bar4 = BarPersist.get_by(uuid=bar4_key)
        foo1.many_to_one = bar1
        foo2.many_to_one = bar1
        foo1.fake_many_to_one = bar1
        foo3.many_to_one = bar2
        foo4.many_to_one = bar2
        foo2.fake_many_to_one = bar2
        foo3.fake_many_to_one = bar3
        foo4.fake_many_to_one = bar4

        import backend.core
        backend.core.commit_session()
        backend.core.remove_session()
        new_active_session()

        count1 = CountOnly.create()
        count1_key = count1.uuid
        count1.increment('inc1', 11)
        count1.increment('inc2', 12)

        count2 = CountOnly.create()
        count2_key = count2.uuid
        count2.increment('inc1', 21)
        count2.increment('inc2', 22)

        count3 = CountOnly.create()
        count3_key = count3.uuid
        count3.increment('inc1', 31)
        count3.increment('inc2', 32)

        count4 = CountOnly.create()
        count4_key = count4.uuid
        count4.increment('inc1', 41)
        count4.increment('inc2', 42)

        key1 = KeyOnly.create()
        key1_key = key1.uuid

        key2 = KeyOnly.create()
        key2_key = key2.uuid

        key3 = KeyOnly.create()
        key3_key = key3.uuid

        key4 = KeyOnly.create()
        key4_key = key4.uuid

        get_active_session().save(cassandra=False, try_later_on_this_thread_first=True)
        new_active_session()

        entities = [p.entity_class for p in collection]
        result = repair_cassandra_from_postgres(
            entity_classes=entities,
            repair=True,
            overwrite=True,
            verify=False)
        self.assertEqual(result, 0)

        def check_original_conditions(check_counters=True):
            # --Foo1-----------------------------------------------------------
            new_active_session()
            foo1 = Foo(foo1_key)
            bar1 = Bar(bar1_key)
            foo1.load(FooLoader)
            self.assertEqual(foo1.str_field, 'foo1')
            self.assertEqual(foo1.uni_field, u'foo1\xe5')
            self.assertEqual(foo1.int_field, 1)
            self.assertEqual(foo1.float_field, 1.1)
            self.assertEqual(foo1.enum_field, FooEnum[0])
            self.assertEqual(foo1.boolean_field, True)
            self.assertEqual(foo1.datetime_field, foo1_datetime)
            self.assertEqual(foo1.date_field, foo1_date)
            self.assertEqual(foo1.time_field, foo1_time)
            if check_counters:
                self.assertEqual(foo1.inc1_field, 11)
                self.assertEqual(foo1.inc2_field, 14)
                self.assertEqual(foo1.inc3_field, 13)
            self.assertEqual(foo1.many_to_one, bar1)
            self.assertEqual(foo1.fake_many_to_one, bar1)

            new_active_session()
            foo1 = Foo(foo1_key)
            loader = FooByDatetimeLoader([FooEnum[0], foo1_datetime])
            loader.load()
            self.assertEqual(foo1.str_field, 'foo1')
            self.assertEqual(foo1.uni_field, u'foo1\xe5')
            self.assertEqual(foo1.int_field, 1)
            self.assertEqual(foo1.float_field, 1.1)
            self.assertEqual(foo1.enum_field, FooEnum[0])
            self.assertEqual(foo1.boolean_field, True)
            self.assertEqual(foo1.datetime_field, foo1_datetime)
            self.assertEqual(foo1.date_field, foo1_date)
            self.assertEqual(foo1.time_field, foo1_time)
            if check_counters:
                self.assertEqual(foo1.inc1_field, 11)
                self.assertEqual(foo1.inc2_field, 14)
                self.assertEqual(foo1.inc3_field, 13)
            self.assertEqual(foo1.many_to_one, bar1)
            self.assertEqual(foo1.fake_many_to_one, bar1)

            new_active_session()
            foo1 = Foo(foo1_key)
            loader = FooByDateLoader([FooEnum[0], foo1_date])
            loader.load()
            self.assertEqual(foo1.str_field, 'foo1')
            self.assertEqual(foo1.uni_field, u'foo1\xe5')
            self.assertEqual(foo1.int_field, 1)
            self.assertEqual(foo1.float_field, 1.1)
            self.assertEqual(foo1.enum_field, FooEnum[0])
            self.assertEqual(foo1.boolean_field, True)
            self.assertEqual(foo1.datetime_field, foo1_datetime)
            self.assertEqual(foo1.date_field, foo1_date)
            self.assertEqual(foo1.time_field, foo1_time)
            if check_counters:
                self.assertEqual(foo1.inc1_field, 11)
                self.assertEqual(foo1.inc2_field, 14)
                self.assertEqual(foo1.inc3_field, 13)
            self.assertEqual(foo1.many_to_one, bar1)
            self.assertEqual(foo1.fake_many_to_one, bar1)

            new_active_session()
            foo1 = Foo(foo1_key)
            loader = FooByTimeLoader([FooEnum[0], foo1_time])
            loader.load()
            self.assertEqual(foo1.str_field, 'foo1')
            self.assertEqual(foo1.uni_field, u'foo1\xe5')
            self.assertEqual(foo1.int_field, 1)
            self.assertEqual(foo1.float_field, 1.1)
            self.assertEqual(foo1.enum_field, FooEnum[0])
            self.assertEqual(foo1.boolean_field, True)
            self.assertEqual(foo1.datetime_field, foo1_datetime)
            self.assertEqual(foo1.date_field, foo1_date)
            self.assertEqual(foo1.time_field, foo1_time)
            if check_counters:
                self.assertEqual(foo1.inc1_field, 11)
                self.assertEqual(foo1.inc2_field, 14)
                self.assertEqual(foo1.inc3_field, 13)
            self.assertEqual(foo1.many_to_one, bar1)
            self.assertEqual(foo1.fake_many_to_one, bar1)

            # --Foo2---------------------------------------------------------------
            new_active_session()
            foo2 = Foo(foo2_key)
            bar1 = Bar(bar1_key)
            bar2 = Bar(bar2_key)
            foo2.load(FooLoader)
            self.assertEqual(foo2.str_field, 'foo2')
            self.assertEqual(foo2.uni_field, u'foo2\xe5')
            self.assertEqual(foo2.int_field, 2)
            self.assertEqual(foo2.float_field, 2.2)
            self.assertEqual(foo2.enum_field, FooEnum[1])
            self.assertEqual(foo2.boolean_field, True)
            self.assertEqual(foo2.datetime_field, foo2_datetime)
            self.assertEqual(foo2.date_field, foo2_date)
            self.assertEqual(foo2.time_field, foo2_time)
            if check_counters:
                self.assertEqual(foo2.inc1_field, 21)
                self.assertEqual(foo2.inc2_field, 24)
                self.assertEqual(foo2.inc3_field, 23)
            self.assertEqual(foo2.many_to_one, bar1)
            self.assertEqual(foo2.fake_many_to_one, bar2)

            new_active_session()
            foo2 = Foo(foo2_key)
            loader = FooByDatetimeLoader([FooEnum[1], foo2_datetime])
            loader.load()
            self.assertEqual(foo2.str_field, 'foo2')
            self.assertEqual(foo2.uni_field, u'foo2\xe5')
            self.assertEqual(foo2.int_field, 2)
            self.assertEqual(foo2.float_field, 2.2)
            self.assertEqual(foo2.enum_field, FooEnum[1])
            self.assertEqual(foo2.boolean_field, True)
            self.assertEqual(foo2.datetime_field, foo2_datetime)
            self.assertEqual(foo2.date_field, foo2_date)
            self.assertEqual(foo2.time_field, foo2_time)
            if check_counters:
                self.assertEqual(foo2.inc1_field, 21)
                self.assertEqual(foo2.inc2_field, 24)
                self.assertEqual(foo2.inc3_field, 23)
            self.assertEqual(foo2.many_to_one, bar1)
            self.assertEqual(foo2.fake_many_to_one, bar2)

            new_active_session()
            foo2 = Foo(foo2_key)
            loader = FooByDateLoader([FooEnum[1], foo2_date])
            loader.load()
            self.assertEqual(foo2.str_field, 'foo2')
            self.assertEqual(foo2.uni_field, u'foo2\xe5')
            self.assertEqual(foo2.int_field, 2)
            self.assertEqual(foo2.float_field, 2.2)
            self.assertEqual(foo2.enum_field, FooEnum[1])
            self.assertEqual(foo2.boolean_field, True)
            self.assertEqual(foo2.datetime_field, foo2_datetime)
            self.assertEqual(foo2.date_field, foo2_date)
            self.assertEqual(foo2.time_field, foo2_time)
            if check_counters:
                self.assertEqual(foo2.inc1_field, 21)
                self.assertEqual(foo2.inc2_field, 24)
                self.assertEqual(foo2.inc3_field, 23)
            self.assertEqual(foo2.many_to_one, bar1)
            self.assertEqual(foo2.fake_many_to_one, bar2)

            new_active_session()
            foo2 = Foo(foo2_key)
            loader = FooByTimeLoader([FooEnum[1], foo2_time])
            loader.load()
            self.assertEqual(foo2.str_field, 'foo2')
            self.assertEqual(foo2.uni_field, u'foo2\xe5')
            self.assertEqual(foo2.int_field, 2)
            self.assertEqual(foo2.float_field, 2.2)
            self.assertEqual(foo2.enum_field, FooEnum[1])
            self.assertEqual(foo2.boolean_field, True)
            self.assertEqual(foo2.datetime_field, foo2_datetime)
            self.assertEqual(foo2.date_field, foo2_date)
            self.assertEqual(foo2.time_field, foo2_time)
            if check_counters:
                self.assertEqual(foo2.inc1_field, 21)
                self.assertEqual(foo2.inc2_field, 24)
                self.assertEqual(foo2.inc3_field, 23)
            self.assertEqual(foo2.many_to_one, bar1)
            self.assertEqual(foo2.fake_many_to_one, bar2)

            # --Foo3---------------------------------------------------------------
            new_active_session()
            foo3 = Foo(foo3_key)
            bar2 = Bar(bar2_key)
            bar3 = Bar(bar3_key)
            foo3.load(FooLoader)
            self.assertEqual(foo3.str_field, 'foo3')
            self.assertEqual(foo3.uni_field, u'foo3\xe5')
            self.assertEqual(foo3.int_field, 3)
            self.assertEqual(foo3.float_field, 3.3)
            self.assertEqual(foo3.enum_field, FooEnum[2])
            self.assertEqual(foo3.boolean_field, True)
            self.assertEqual(foo3.datetime_field, None)
            self.assertEqual(foo3.date_field, None)
            self.assertEqual(foo3.time_field, None)
            if check_counters:
                self.assertEqual(foo3.inc1_field, 31)
                self.assertEqual(foo3.inc2_field, 34)
                self.assertEqual(foo3.inc3_field, 33)
            self.assertEqual(foo3.many_to_one, bar2)
            self.assertEqual(foo3.fake_many_to_one, bar3)

            # --Foo4---------------------------------------------------------------
            new_active_session()
            foo4 = Foo(foo4_key)
            bar2 = Bar(bar2_key)
            bar4 = Bar(bar4_key)
            foo4.load(FooLoader)
            self.assertEqual(foo4.str_field, 'foo4')
            self.assertEqual(foo4.uni_field, u'foo4\xe5')
            self.assertEqual(foo4.int_field, 4)
            self.assertEqual(foo4.float_field, 4.4)
            self.assertEqual(foo4.enum_field, FooEnum[0])
            self.assertEqual(foo4.boolean_field, True)
            self.assertEqual(foo4.datetime_field, None)
            self.assertEqual(foo4.date_field, None)
            self.assertEqual(foo4.time_field, None)
            if check_counters:
                self.assertEqual(foo4.inc1_field, 41)
                self.assertEqual(foo4.inc2_field, 44)
                self.assertEqual(foo4.inc3_field, 43)
            self.assertEqual(foo4.many_to_one, bar2)
            self.assertEqual(foo4.fake_many_to_one, bar4)

        check_original_conditions(check_counters=False)

        result = repair_cassandra_from_postgres(entity_classes=entities)
        self.assertEqual(result, 0)

        # The make a few common discrepencies in Cassandra.
        # foo1 is normal
        # foo2 is missing two of three indexs and one incr field, and a scalar error.
        # foo3 is missing its incr CF and a few scalar fields.
        # foo4 does not exist at all.

        def prepare_cassandra():
            # foo2 is missing two of three indexs and one incr field, and a scalar error.
            util.cassandra.batch(deletes={
                'b_foo_bydatetime': [{'uuid': foo2_key, 'datetime_field': to_hsntime(foo2_datetime), 'enum_field': 1}],
                'b_foo_bytime': [{'uuid': foo2_key, 'time_field': to_timestorage(foo2_time), 'enum_field': 1}]
            })
            # I don't know how to delete a counter column so let's delete the CF
            # and remake it.
            # NOTE: this behavior is undefined in Cassandra.
            util.cassandra.batch(deletes={
                'b_foo__counts': [{'uuid': foo2_key}]
            })
            util.cassandra.batch(increments={
                ('b_foo__counts', ('uuid',),): [{'uuid': foo2_key, 'inc1_field': 21, 'inc3_field': 23}]
            })
            util.cassandra.batch(cfs_data_pairs={
                ('b_foo', ('uuid',),): [{'uuid': foo2_key, 'uni_field': u'fooxxx\xe5'}]
            })

            # foo3 is missing its incr CF and a few scalar fields.
            util.cassandra.batch(deletes={
                'b_foo__counts': [{'uuid': foo3_key}]
            })
            util.cassandra.batch(cfs_data_pairs={
                ('b_foo', ('uuid',),): [{'uuid': foo3_key,
                                         'uni_field': None,
                                         'float_field': None}]
            })

            # foo4 does not exist at all.
            util.cassandra.batch(deletes={
                'b_foo__counts': [{'uuid': foo4_key}],
                'b_foo': [{'uuid': foo4_key}]
            })

        prepare_cassandra()

        # --Foo1---------------------------------------------------------------
        new_active_session()
        foo1 = Foo(foo1_key)
        bar1 = Bar(bar1_key)
        foo1.load(FooLoader)
        self.assertEqual(foo1.str_field, 'foo1')
        self.assertEqual(foo1.uni_field, u'foo1\xe5')
        self.assertEqual(foo1.int_field, 1)
        self.assertEqual(foo1.float_field, 1.1)
        self.assertEqual(foo1.enum_field, FooEnum[0])
        self.assertEqual(foo1.boolean_field, True)
        self.assertEqual(foo1.datetime_field, foo1_datetime)
        self.assertEqual(foo1.date_field, foo1_date)
        self.assertEqual(foo1.time_field, foo1_time)
        self.assertEqual(foo1.inc1_field, 11)
        self.assertEqual(foo1.inc2_field, 14)
        self.assertEqual(foo1.inc3_field, 13)
        self.assertEqual(foo1.many_to_one, bar1)
        self.assertEqual(foo1.fake_many_to_one, bar1)

        new_active_session()
        foo1 = Foo(foo1_key)
        loader = FooByDatetimeLoader([FooEnum[0], foo1_datetime])
        loader.load()
        self.assertEqual(foo1.str_field, 'foo1')
        self.assertEqual(foo1.uni_field, u'foo1\xe5')
        self.assertEqual(foo1.int_field, 1)
        self.assertEqual(foo1.float_field, 1.1)
        self.assertEqual(foo1.enum_field, FooEnum[0])
        self.assertEqual(foo1.boolean_field, True)
        self.assertEqual(foo1.datetime_field, foo1_datetime)
        self.assertEqual(foo1.date_field, foo1_date)
        self.assertEqual(foo1.time_field, foo1_time)
        self.assertEqual(foo1.inc1_field, 11)
        self.assertEqual(foo1.inc2_field, 14)
        self.assertEqual(foo1.inc3_field, 13)
        self.assertEqual(foo1.many_to_one, bar1)
        self.assertEqual(foo1.fake_many_to_one, bar1)

        new_active_session()
        foo1 = Foo(foo1_key)
        loader = FooByDateLoader([FooEnum[0], foo1_date])
        loader.load()
        self.assertEqual(foo1.str_field, 'foo1')
        self.assertEqual(foo1.uni_field, u'foo1\xe5')
        self.assertEqual(foo1.int_field, 1)
        self.assertEqual(foo1.float_field, 1.1)
        self.assertEqual(foo1.enum_field, FooEnum[0])
        self.assertEqual(foo1.boolean_field, True)
        self.assertEqual(foo1.datetime_field, foo1_datetime)
        self.assertEqual(foo1.date_field, foo1_date)
        self.assertEqual(foo1.time_field, foo1_time)
        self.assertEqual(foo1.inc1_field, 11)
        self.assertEqual(foo1.inc2_field, 14)
        self.assertEqual(foo1.inc3_field, 13)
        self.assertEqual(foo1.many_to_one, bar1)
        self.assertEqual(foo1.fake_many_to_one, bar1)

        new_active_session()
        foo1 = Foo(foo1_key)
        loader = FooByTimeLoader([FooEnum[0], foo1_time])
        loader.load()
        self.assertEqual(foo1.str_field, 'foo1')
        self.assertEqual(foo1.uni_field, u'foo1\xe5')
        self.assertEqual(foo1.int_field, 1)
        self.assertEqual(foo1.float_field, 1.1)
        self.assertEqual(foo1.enum_field, FooEnum[0])
        self.assertEqual(foo1.boolean_field, True)
        self.assertEqual(foo1.datetime_field, foo1_datetime)
        self.assertEqual(foo1.date_field, foo1_date)
        self.assertEqual(foo1.time_field, foo1_time)
        self.assertEqual(foo1.inc1_field, 11)
        self.assertEqual(foo1.inc2_field, 14)
        self.assertEqual(foo1.inc3_field, 13)
        self.assertEqual(foo1.many_to_one, bar1)
        self.assertEqual(foo1.fake_many_to_one, bar1)

        # --Foo2---------------------------------------------------------------
        new_active_session()
        foo2 = Foo(foo2_key)
        bar1 = Bar(bar1_key)
        bar2 = Bar(bar2_key)
        foo2.load(FooLoader)
        self.assertEqual(foo2.str_field, 'foo2')
        self.assertEqual(foo2.uni_field, u'fooxxx\xe5')
        self.assertEqual(foo2.int_field, 2)
        self.assertEqual(foo2.float_field, 2.2)
        self.assertEqual(foo2.enum_field, FooEnum[1])
        self.assertEqual(foo2.boolean_field, True)
        self.assertEqual(foo2.datetime_field, foo2_datetime)
        self.assertEqual(foo2.date_field, foo2_date)
        self.assertEqual(foo2.time_field, foo2_time)
        self.assertEqual(foo2.inc1_field, 0)
        self.assertEqual(foo2.inc2_field, 0)
        self.assertEqual(foo2.inc3_field, 0)
        self.assertEqual(foo2.many_to_one, bar1)
        self.assertEqual(foo2.fake_many_to_one, bar2)

        new_active_session()
        foo2 = Foo(foo2_key)
        loader = FooByDatetimeLoader([FooEnum[1], foo2_datetime])
        loader.load()
        self.assertEqual(len(loader.result), 0)

        new_active_session()
        foo2 = Foo(foo2_key)
        loader = FooByDateLoader([FooEnum[1], foo2_date])
        loader.load()
        self.assertEqual(foo2.str_field, 'foo2')
        self.assertEqual(foo2.uni_field, u'foo2\xe5')
        self.assertEqual(foo2.int_field, 2)
        self.assertEqual(foo2.float_field, 2.2)
        self.assertEqual(foo2.enum_field, FooEnum[1])
        self.assertEqual(foo2.boolean_field, True)
        self.assertEqual(foo2.datetime_field, foo2_datetime)
        self.assertEqual(foo2.date_field, foo2_date)
        self.assertEqual(foo2.time_field, foo2_time)
        self.assertEqual(foo2.inc1_field, 0)
        self.assertEqual(foo2.inc2_field, 0)
        self.assertEqual(foo2.inc3_field, 0)
        self.assertEqual(foo2.many_to_one, bar1)
        self.assertEqual(foo2.fake_many_to_one, bar2)

        new_active_session()
        foo2 = Foo(foo2_key)
        loader = FooByTimeLoader([FooEnum[1], foo2_time])
        loader.load()
        self.assertEqual(len(loader.result), 0)

        # --Foo3---------------------------------------------------------------
        new_active_session()
        foo3 = Foo(foo3_key)
        bar2 = Bar(bar2_key)
        bar3 = Bar(bar3_key)
        foo3.load(FooLoader)
        self.assertEqual(foo3.str_field, 'foo3')
        self.assertEqual(foo3.uni_field, None)
        self.assertEqual(foo3.int_field, 3)
        self.assertEqual(foo3.float_field, None)
        self.assertEqual(foo3.enum_field, FooEnum[2])
        self.assertEqual(foo3.boolean_field, True)
        self.assertEqual(foo3.datetime_field, None)
        self.assertEqual(foo3.date_field, None)
        self.assertEqual(foo3.time_field, None)
        self.assertEqual(foo3.inc1_field, 0)
        self.assertEqual(foo3.inc2_field, 0)
        self.assertEqual(foo3.inc3_field, 0)
        self.assertEqual(foo3.many_to_one, bar2)
        self.assertEqual(foo3.fake_many_to_one, bar3)

        # --Foo4---------------------------------------------------------------
        new_active_session()
        foo4 = Foo(foo4_key)
        raised = False
        try:
            foo4.load(FooLoader)
        except NullEnvironmentError:
            raised =True
        self.assertEqual(raised, True)

        count = repair_cassandra_from_postgres(entity_classes=entities)
        self.assertTrue(count > 0)

        # Confirm that the repair did not alter anything.

        # --Foo1---------------------------------------------------------------
        new_active_session()
        foo1 = Foo(foo1_key)
        bar1 = Bar(bar1_key)
        foo1.load(FooLoader)
        self.assertEqual(foo1.str_field, 'foo1')
        self.assertEqual(foo1.uni_field, u'foo1\xe5')
        self.assertEqual(foo1.int_field, 1)
        self.assertEqual(foo1.float_field, 1.1)
        self.assertEqual(foo1.enum_field, FooEnum[0])
        self.assertEqual(foo1.boolean_field, True)
        self.assertEqual(foo1.datetime_field, foo1_datetime)
        self.assertEqual(foo1.date_field, foo1_date)
        self.assertEqual(foo1.time_field, foo1_time)
        self.assertEqual(foo1.inc1_field, 11)
        self.assertEqual(foo1.inc2_field, 14)
        self.assertEqual(foo1.inc3_field, 13)
        self.assertEqual(foo1.many_to_one, bar1)
        self.assertEqual(foo1.fake_many_to_one, bar1)

        new_active_session()
        foo1 = Foo(foo1_key)
        loader = FooByDatetimeLoader([FooEnum[0], foo1_datetime])
        loader.load()
        self.assertEqual(foo1.str_field, 'foo1')
        self.assertEqual(foo1.uni_field, u'foo1\xe5')
        self.assertEqual(foo1.int_field, 1)
        self.assertEqual(foo1.float_field, 1.1)
        self.assertEqual(foo1.enum_field, FooEnum[0])
        self.assertEqual(foo1.boolean_field, True)
        self.assertEqual(foo1.datetime_field, foo1_datetime)
        self.assertEqual(foo1.date_field, foo1_date)
        self.assertEqual(foo1.time_field, foo1_time)
        self.assertEqual(foo1.inc1_field, 11)
        self.assertEqual(foo1.inc2_field, 14)
        self.assertEqual(foo1.inc3_field, 13)
        self.assertEqual(foo1.many_to_one, bar1)
        self.assertEqual(foo1.fake_many_to_one, bar1)

        new_active_session()
        foo1 = Foo(foo1_key)
        loader = FooByDateLoader([FooEnum[0], foo1_date])
        loader.load()
        self.assertEqual(foo1.str_field, 'foo1')
        self.assertEqual(foo1.uni_field, u'foo1\xe5')
        self.assertEqual(foo1.int_field, 1)
        self.assertEqual(foo1.float_field, 1.1)
        self.assertEqual(foo1.enum_field, FooEnum[0])
        self.assertEqual(foo1.boolean_field, True)
        self.assertEqual(foo1.datetime_field, foo1_datetime)
        self.assertEqual(foo1.date_field, foo1_date)
        self.assertEqual(foo1.time_field, foo1_time)
        self.assertEqual(foo1.inc1_field, 11)
        self.assertEqual(foo1.inc2_field, 14)
        self.assertEqual(foo1.inc3_field, 13)
        self.assertEqual(foo1.many_to_one, bar1)
        self.assertEqual(foo1.fake_many_to_one, bar1)

        new_active_session()
        foo1 = Foo(foo1_key)
        loader = FooByTimeLoader([FooEnum[0], foo1_time])
        loader.load()
        self.assertEqual(foo1.str_field, 'foo1')
        self.assertEqual(foo1.uni_field, u'foo1\xe5')
        self.assertEqual(foo1.int_field, 1)
        self.assertEqual(foo1.float_field, 1.1)
        self.assertEqual(foo1.enum_field, FooEnum[0])
        self.assertEqual(foo1.boolean_field, True)
        self.assertEqual(foo1.datetime_field, foo1_datetime)
        self.assertEqual(foo1.date_field, foo1_date)
        self.assertEqual(foo1.time_field, foo1_time)
        self.assertEqual(foo1.inc1_field, 11)
        self.assertEqual(foo1.inc2_field, 14)
        self.assertEqual(foo1.inc3_field, 13)
        self.assertEqual(foo1.many_to_one, bar1)
        self.assertEqual(foo1.fake_many_to_one, bar1)

        # --Foo2---------------------------------------------------------------
        new_active_session()
        foo2 = Foo(foo2_key)
        bar1 = Bar(bar1_key)
        bar2 = Bar(bar2_key)
        foo2.load(FooLoader)
        self.assertEqual(foo2.str_field, 'foo2')
        self.assertEqual(foo2.uni_field, u'fooxxx\xe5')
        self.assertEqual(foo2.int_field, 2)
        self.assertEqual(foo2.float_field, 2.2)
        self.assertEqual(foo2.enum_field, FooEnum[1])
        self.assertEqual(foo2.boolean_field, True)
        self.assertEqual(foo2.datetime_field, foo2_datetime)
        self.assertEqual(foo2.date_field, foo2_date)
        self.assertEqual(foo2.time_field, foo2_time)
        self.assertEqual(foo2.inc1_field, 0)
        self.assertEqual(foo2.inc2_field, 0)
        self.assertEqual(foo2.inc3_field, 0)
        self.assertEqual(foo2.many_to_one, bar1)
        self.assertEqual(foo2.fake_many_to_one, bar2)

        new_active_session()
        foo2 = Foo(foo2_key)
        loader = FooByDatetimeLoader([FooEnum[1], foo2_datetime])
        loader.load()
        self.assertEqual(len(loader.result), 0)

        new_active_session()
        foo2 = Foo(foo2_key)
        loader = FooByDateLoader([FooEnum[1], foo2_date])
        loader.load()
        self.assertEqual(foo2.str_field, 'foo2')
        self.assertEqual(foo2.uni_field, u'foo2\xe5')
        self.assertEqual(foo2.int_field, 2)
        self.assertEqual(foo2.float_field, 2.2)
        self.assertEqual(foo2.enum_field, FooEnum[1])
        self.assertEqual(foo2.boolean_field, True)
        self.assertEqual(foo2.datetime_field, foo2_datetime)
        self.assertEqual(foo2.date_field, foo2_date)
        self.assertEqual(foo2.time_field, foo2_time)
        self.assertEqual(foo2.inc1_field, 0)
        self.assertEqual(foo2.inc2_field, 0)
        self.assertEqual(foo2.inc3_field, 0)
        self.assertEqual(foo2.many_to_one, bar1)
        self.assertEqual(foo2.fake_many_to_one, bar2)

        new_active_session()
        foo2 = Foo(foo2_key)
        loader = FooByTimeLoader([FooEnum[1], foo2_time])
        loader.load()
        self.assertEqual(len(loader.result), 0)

        # --Foo3---------------------------------------------------------------
        new_active_session()
        foo3 = Foo(foo3_key)
        bar2 = Bar(bar2_key)
        bar3 = Bar(bar3_key)
        foo3.load(FooLoader)
        self.assertEqual(foo3.str_field, 'foo3')
        self.assertEqual(foo3.uni_field, None)
        self.assertEqual(foo3.int_field, 3)
        self.assertEqual(foo3.float_field, None)
        self.assertEqual(foo3.enum_field, FooEnum[2])
        self.assertEqual(foo3.boolean_field, True)
        self.assertEqual(foo3.datetime_field, None)
        self.assertEqual(foo3.date_field, None)
        self.assertEqual(foo3.time_field, None)
        self.assertEqual(foo3.inc1_field, 0)
        self.assertEqual(foo3.inc2_field, 0)
        self.assertEqual(foo3.inc3_field, 0)
        self.assertEqual(foo3.many_to_one, bar2)
        self.assertEqual(foo3.fake_many_to_one, bar3)

        # --Foo4---------------------------------------------------------------
        new_active_session()
        foo4 = Foo(foo4_key)
        raised = False
        try:
            foo4.load(FooLoader)
        except NullEnvironmentError:
            raised =True
        self.assertEqual(raised, True)

        new_active_session()

        # Let's do the repair.
        result = repair_cassandra_from_postgres(
            entity_classes=entities,
            repair=True,
            overwrite=True,
            verify=False)
        self.assertEqual(result, 0)

        check_original_conditions(check_counters=False)
