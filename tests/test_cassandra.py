
from datetime import date, datetime, time

import elixir
import elixir.collection
import elixir.entity
from sqlalchemy import MetaData, create_engine
import sqlalchemy.orm

import util.database
from util.cassandra import batch, Fetcher

from util.when import to_timestorage, to_hsndate, to_hsntime
import backend
from backend import new_active_session, get_active_session, generate_id
from backend.environments.cache import Cache
from backend.environments.core import Entity
from backend.exceptions import AllAttributesNotLoaded, \
                               AssignmentNotAllowed, \
                               AttributeNotLoaded, \
                               EmptyKeyError, \
                               KeyNotLoaded, \
                               NullEnvironmentError, \
                               UninitializedLoader
from backend.loader import CompositeKeyLoader, InventoryLoader, load
from backend.schema import DateField, DatetimeField, Persistent, Enum, \
    UnicodeField, UUIDField, StringField, LongField, EnumField, \
    EnumSetField, IncrementField, IntegerField, FloatField, BooleanField, \
    OneToManyField, ManyToOneField, FakeManyToOneField, OneToOneField, \
    TimeField, TimeUUIDField, nnps_get, nnps_set
from backend.tests import TestCase, _setup_module, _teardown_module

db_name = 'test_cassandra'

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
    cache_list = [BridgeCache]
    loader_list = [FooLoader, FooSmallLoader, FooOtherSmallLoader,
                            FooBothCountLoader, FooCountLoader, BazLoader,
                            StreamLoader, AlphaLoader, PostLoader,
                            PostKeyLoader, SmallLoaderA, SmallLoaderB,
                            SmallLoaderC, SmallLoaderD, TimeTextLoader,
                            IncNone1Loader, IncNone2Loader, IncNoneBothLoader]

    _setup_module(db_name, collection, metadata, session, cache_list, loader_list)


def teardown_module():
    _teardown_module(db_name, collection, metadata, session, engine)

#Model Classes needed for Test
#Define and Setup Test Database

FooEnum = Enum(u'FooEnum', [u'frob', u'grob', u'blob'])
PostEnum = Enum(u'PostEnum', ['unflagged', 'flagged', 'popular'])

class EnumSetPersist(Persistent):
    elixir.using_options(tablename='enumset', collection=collection)
    uuid = UUIDField(primary_key=True, autoload=True)
    number = IntegerField()
    user = ManyToOneField('FooPersist', inverse='enum_set_field')

class EnumSet(Entity):
    persistent_class = EnumSetPersist

EnumSet.entity_class = EnumSet
EnumSetPersist.entity_class = EnumSet
EnumSetPersist.persistent_class = EnumSetPersist

class CommentPersist(Persistent):
    elixir.using_options(tablename='comments', collection=collection)
    uuid = UUIDField(primary_key=True)
    text = UnicodeField(autoload=True)
    post = ManyToOneField('PostPersist', inverse='comments')

class Comment(Entity):
    persistent_class = CommentPersist
Comment.persistent_class = CommentPersist
Comment.entity_class = Comment
CommentPersist.entity_class = Comment
CommentPersist.persistent_class = CommentPersist

class UserPersist(Persistent):
    elixir.using_options(tablename='users', collection=collection)
    uuid = UUIDField(primary_key=True)
    name = UnicodeField(autoload=True)
    posts = OneToManyField('PostPersist', inverse='user')
    other_posts = OneToManyField('PostPersist', inverse='other_user')

class User(Entity):
    persistent_class = UserPersist
User.persistent_class = UserPersist
User.entity_class = User
UserPersist.entity_class = User
UserPersist.persistent_class = UserPersist

class PostPersist(Persistent):
    elixir.using_options(tablename='posts', collection=collection)
    uuid = UUIDField(primary_key=True)
    posted_on = DatetimeField()
    text = UnicodeField()
    user = ManyToOneField('UserPersist', inverse='posts')
    # Needed a related object that could be None (user is in composite key
    # so it can't be None)
    other_user = ManyToOneField('UserPersist', inverse='other_posts')
    other_text = UnicodeField()
    comments = OneToManyField('CommentPersist', inverse='post')
    kind = EnumField(PostEnum, default=PostEnum[0])
#    kind_with_none_default = EnumField(PostEnum, default=None)
    rank = IntegerField(default=0)
    upvotes = IncrementField(default=0)
    indexes = {
        'stream': ('user', 'posted_on'),
        'alpha': ('text', 'kind', 'rank'),
        'withprimary': ('user', 'uuid') # We had issues where the primary
        # key, if it were in a composite key index, would bork update.
    }

class Post(Entity):
    persistent_class = PostPersist
Post.persistent_class = PostPersist
Post.entity_class = Post
PostPersist.entity_class = Post
PostPersist.persistent_class = PostPersist

class SmallPersist(Persistent):
    elixir.using_options(tablename='small', collection=collection)
    uuid = UUIDField(primary_key=True)
    text1 = UnicodeField()
    text2 = UnicodeField()
    text3 = UnicodeField()
    inc1 = IncrementField(default=0)
    inc2 = IncrementField(default=0)
    inc3 = IncrementField(default=0)
    inc4 = IncrementField(default=None)

class Small(Entity):
    persistent_class = SmallPersist
Small.persistent_class = SmallPersist
Small.entity_class = Small
SmallPersist.entity_class = Small
SmallPersist.persistent_class = SmallPersist

class IncNonePersist(Persistent):
    elixir.using_options(tablename='incnone', collection=collection)
    uuid = UUIDField(primary_key=True)
    inc1 = IncrementField(default=None)
    inc2 = IncrementField(default=None)

class IncNone(Entity):
    persistent_class = IncNonePersist
IncNone.persistent_class = IncNonePersist
IncNone.entity_class = IncNone
IncNonePersist.entity_class = IncNone
IncNonePersist.persistent_class = IncNonePersist


class FooPersist(Persistent):
    elixir.using_options(tablename='foo_object', collection=collection)
    uuid = UUIDField(primary_key=True)
    bridge1 = OneToManyField('BridgePersist', inverse='foo1')
    bridge2 = OneToManyField('BridgePersist', inverse='foo2')
    baz_many = ManyToOneField('BazPersist', inverse='foos')
    baz_one = FakeManyToOneField('BazPersist', inverse='foo')
    str_field = StringField()
    uni_field = UnicodeField()
    enum_field = EnumField(FooEnum)
    enum_set_field = EnumSetField('EnumSetPersist',
                                   FooEnum,
                                   inverse='user')
    int_field = IntegerField(default=0)
    long_field = LongField(default=0L)
    inc_field = IncrementField(default=0)
    inc2_field = IncrementField(default=10)
    float_field = FloatField(default=0.0)
    boolean_field = BooleanField()
    date_field = DateField()
    datetime_field = DatetimeField()
    time_field = TimeField()

class Foo(Entity):
    _enum_set_field = property(nnps_get(FooEnum, 'enum_set_field'),
                                    nnps_set(FooEnum,
                                             'enum_set_field',
                                             EnumSet,
                                             'user'))
Foo.persistent_class = FooPersist
Foo.entity_class = Foo
FooPersist.entity_class = Foo
FooPersist.persistent_class = FooPersist

class BarPersist(Persistent):
    elixir.using_options(tablename='bar_object',
                         collection=collection)
    uuid = UUIDField(primary_key=True)
    bridge = OneToManyField('BridgePersist', inverse='bar')
    name1 = UnicodeField()
    name2 = UnicodeField()

class Bar(Entity):
    consistency = 'ALL'
Bar.persistent_class = BarPersist
Bar.entity_class = Bar
BarPersist.entity_class = Bar
BarPersist.persistent_class = BarPersist


class BazPersist(Persistent):
    elixir.using_options(tablename='baz_object', collection=collection)
    uuid = UUIDField(primary_key=True)
    foos = OneToManyField('FooPersist', inverse='baz_many')
    foo = OneToOneField('FooPersist', inverse='baz_one')
    int_field = IntegerField(default=0)
    uni_field = UnicodeField()

class Baz(Entity):
    pass
Baz.persistent_class = BazPersist
Baz.entity_class = Baz
BazPersist.entity_class = Baz
BazPersist.persistent_class = BazPersist

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

class TimePersist(Persistent):
    elixir.using_options(tablename='time_object',
                         collection=collection)
    uuid = TimeUUIDField(primary_key=True)
    text = UnicodeField()
    many_to_one = ManyToOneField('TimePersist', inverse='one_to_many')
    one_to_many = OneToManyField('TimePersist', inverse='many_to_one')
    indexes = {
        'stream': ('text', 'uuid')
    }

class Time(Entity):
    pass
Time.persistent_class = TimePersist
Time.entity_class = Time
TimePersist.entity_class = Time
TimePersist.persistent_class = TimePersist

class FooLoader(InventoryLoader):
    entity_class = Foo
    inventory = [
        'uuid',
        #['bridge1', [['bar',['name1']]]],
        #['bridge2', [['bar',['name2']]]]]
        'str_field',
        'uni_field',
        'enum_field',
        'int_field',
        'long_field',
        'float_field',
        'boolean_field',
        ['baz_many', ['int_field']],
        ['baz_one', ['uni_field']],
        'inc_field',
        'date_field',
        'datetime_field',
        'time_field'
    ]

class FooSmallLoader(InventoryLoader):
    consistency = 'EACH_QUORUM'
    entity_class = Foo
    inventory = [
        'uuid',
        #['bridge1', [['bar',['name1']]]],
        #['bridge2', [['bar',['name2']]]]]
        'str_field',
        'uni_field',
        'enum_field',
        'int_field',
        'long_field',
        'float_field',
        'boolean_field',
        ['baz_many', ['int_field']]
    ]

class FooOtherSmallLoader(InventoryLoader):
    consistency = 'ALL'
    entity_class = Foo
    inventory = [
        'uuid',
        ['baz_one', ['uni_field']],
        'inc_field',
        'date_field',
        'datetime_field',
        'time_field'
    ]

class FooCountLoader(InventoryLoader):
    entity_class = Foo
    inventory = [
        'inc2_field'
    ]

class FooBothCountLoader(InventoryLoader):
    entity_class = Foo
    inventory = [
        'inc_field',
        'inc2_field'
    ]

class UninitializedFooLoader(InventoryLoader):
    entity_class = Foo
    inventory = [
        'uuid',
        #['bridge1', [['bar',['name1']]]],
        #['bridge2', [['bar',['name2']]]]]
        'str_field',
        'uni_field',
        'enum_field',
        'int_field',
        'long_field',
        'float_field',
        'boolean_field',
        ['baz_many', ['int_field']],
        ['baz_one', ['uni_field']],
        'inc_field',
        'date_field',
        'datetime_field',
        'time_field'
    ]

class SmallLoaderA(InventoryLoader):
    entity_class = Small
    inventory = [
        'uuid',
        'text1',
        'inc3'
    ]

class SmallLoaderB(InventoryLoader):
    entity_class = Small
    inventory = [
        'uuid',
        'text2',
        'inc1',
        'inc2'
    ]

class SmallLoaderC(InventoryLoader):
    entity_class = Small
    inventory = [
        'uuid',
        'text3',
        'inc2'
    ]

class SmallLoaderD(InventoryLoader):
    entity_class = Small
    inventory = [
        'uuid',
        'text1',
        'text2',
        'inc1'
    ]

class IncNone1Loader(InventoryLoader):
    entity_class = IncNone
    inventory = [
        'inc1'
    ]

class IncNone2Loader(InventoryLoader):
    entity_class = IncNone
    inventory = [
        'inc2'
    ]

class IncNoneBothLoader(InventoryLoader):
    entity_class = IncNone
    inventory = [
        'inc1',
        'inc2'
    ]

class PostLoader(InventoryLoader):
    entity_class = Post
    inventory = [
        'uuid',
        'user',
        'other_user',
        'other_text',
        'text',
        'kind',
#        'kind_with_default',
        'rank',
        'posted_on'
    ]

class PostKeyLoader(InventoryLoader):
    entity_class = Post
    inventory = [
        'uuid',
        'user',
        'text',
        'kind',
        'rank',
        'posted_on'
    ]

class StreamLoader(CompositeKeyLoader):
    entity_class = Post
    composite_key_name = 'stream'
    inventory = [
        'uuid',
        'user',
        'other_user',
        'posted_on',
        'text',
        'upvotes'
    ]

class AlphaLoader(CompositeKeyLoader):
    entity_class = Post
    composite_key_name = 'alpha'
    inventory = [
        'uuid',
        'user',
        'other_user',
        'posted_on',
        'text'
    ]

class BridgeCache(Cache):
    entity_class = Bridge
    inventory = [
        'uuid', 'foo1', 'foo2', 'bar'
    ]

class BazLoader(InventoryLoader):
    entity_class = Baz
    inventory = ['uuid', 'int_field', 'uni_field']

class TimeTextLoader(CompositeKeyLoader):
    entity_class = Time
    composite_key_name = 'stream'
    inventory = [
        'uuid',
        'text'
    ]


def get_cassandra(entity, attribute_name):
    column_family = entity.entity_class.get_primary_column_family()
    return get_cassandra_raw(column_family, attribute_name, entity.entity_class.key_attribute_name, entity.get_key_attribute().cassandra_encode(entity.key))

def get_cassandra_raw(column_family, attribute_name, key_attribute_name, encoded_key):
    fetcher = Fetcher(cf_name=column_family, fields=(attribute_name,))
    fetcher.add_column_value_relation(key_attribute_name, encoded_key)
    result = fetcher.fetch_first(connection=None, cursor=None, retries=0, consistency_level=None)
    if result:
        return result[0]
    else:
        return None

def get_cassandra_all(column_family, attribute_name):
    fetcher = Fetcher(cf_name=column_family, fields=(attribute_name,), limit=100)
    return [r[0] for r in fetcher.fetch_all()]

def save():
    get_active_session().save(try_later_on_this_thread_first=True)
    new_active_session()

class TestCassandra(TestCase):
    collection = collection

    def assertNotLoaded(self, entity, attrname):
        raised = False
        try:
            getattr(entity, attrname)
        except AttributeNotLoaded:
            raised = True
        self.assertTrue(raised)

    def test_set_attributes_to_none(self):
        """Set attributes to None."""
        self.reset_cassandra([Foo, EnumSet, Post, User])
        # Create Persistent object.
        foo = Foo.create()
        foo_key = foo.key
        foo.str_field = 'strabcd'
        foo.uni_field = u'uni & abcd'
        foo.int_field = 22
        foo.long_field = 50000000000L
        foo.float_field = 3.14159265
        foo.enum_field = FooEnum[u'grob']
        foo._enum_set_field = [FooEnum[u'frob'], FooEnum[u'blob']]
        foo.boolean_field = True
        foo.date_field = date(1998, 12, 31)
        foo.datetime_field = datetime(2001, 5, 17, 22, 58, 59)
        foo.time_field = time(20, 58, 59)

        save()

        foo_persist = FooPersist.get_by(uuid=foo_key)
        self.assertEqual(u'strabcd', foo_persist.str_field)
        self.assertEqual(u'uni & abcd', foo_persist.uni_field)
        self.assertEqual(FooEnum[u'grob'], foo_persist.enum_field)
        self.assertEqual(22, foo_persist.int_field)
        self.assertEqual(50000000000L, foo_persist.long_field)
        self.assertEqual(3.14159265, foo_persist.float_field)
        self.assertTrue(foo_persist.boolean_field)
        self.assertEqual(None, foo_persist.baz_many)
        self.assertEqual(None, foo_persist.baz_one)
        self.assertEqual(date(1998, 12, 31), foo_persist.date_field)
        self.assertEqual(datetime(2001, 5, 17, 22, 58, 59), foo_persist.datetime_field)
        self.assertEqual(time(20, 58, 59), foo_persist.time_field)

        foo.load(FooLoader)
        self.assertEqual(u'strabcd', foo.str_field)
        self.assertEqual(u'uni & abcd', foo.uni_field)
        self.assertEqual(FooEnum[u'grob'], foo.enum_field)
        self.assertEqual(22, foo.int_field)
        self.assertEqual(50000000000L, foo.long_field)
        self.assertEqual(3.14159265, foo.float_field)
        self.assertTrue(foo.boolean_field)
        self.assertEqual(None, foo.baz_many)
        self.assertEqual(None, foo.baz_one)
        self.assertEqual(date(1998, 12, 31), foo.date_field)
        self.assertEqual(datetime(2001, 5, 17, 22, 58, 59), foo.datetime_field)
        self.assertEqual(time(20, 58, 59), foo.time_field)

        foo = Foo(foo_key)
        foo.str_field = None
        foo.uni_field = None
        foo.int_field = None
        foo.long_field = None
        foo.float_field = None
        foo.enum_field = None
        foo.boolean_field = None
        foo.date_field = None
        foo.datetime_field = None
        foo.time_field = None

        save()

        foo_persist = FooPersist.get_by(uuid=foo_key)
        self.assertEqual(None, foo_persist.str_field)
        self.assertEqual(None, foo_persist.uni_field)
        self.assertEqual(None, foo_persist.enum_field)
        self.assertEqual(None, foo_persist.int_field)
        self.assertEqual(None, foo_persist.long_field)
        self.assertEqual(None, foo_persist.float_field)
        self.assertEqual(None, foo_persist.boolean_field)
        self.assertEqual(None, foo_persist.baz_many)
        self.assertEqual(None, foo_persist.baz_one)
        self.assertEqual(None, foo_persist.date_field)
        self.assertEqual(None, foo_persist.datetime_field)
        self.assertEqual(None, foo_persist.time_field)

        foo = Foo(foo_key)
        foo.load(FooLoader)
        self.assertEqual(None, foo.str_field)
        self.assertEqual(None, foo.uni_field)
        self.assertEqual(foo.get_attribute('enum_field').enum[0], foo.enum_field)#None, foo.enum_field)         # xxx put this back after migrating away NOne enums.
        self.assertEqual(None, foo.int_field)
        self.assertEqual(None, foo.long_field)
        self.assertEqual(None, foo.float_field)
        self.assertEqual(None, foo.boolean_field)
        self.assertEqual(None, foo.baz_many)
        self.assertEqual(None, foo.baz_one)
        self.assertEqual(None, foo.date_field)
        self.assertEqual(None, foo.datetime_field)
        self.assertEqual(None, foo.time_field)

        foo.str_field = 'strabcd'
        foo.uni_field = u'uni & abcd'
        foo.int_field = 22
        foo.long_field = 50000000000L
        foo.float_field = 3.14159265
        foo.enum_field = FooEnum[u'grob']
        # Note if you set this again it'll just create more entries in
        # cassandra and postgres and leave the old ones too.
#        foo._enum_set_field = [FooEnum[u'grob'], FooEnum[u'blob']]
        foo.boolean_field = True
        foo.date_field = date(1998, 12, 31)
        foo.datetime_field = datetime(2001, 5, 17, 22, 58, 59)
        foo.time_field = time(20, 58, 59)

        save()
        # left this in as notes.  They are a way of checking what the second
        # setting of _enum_set_field does.
#        foo_persist = FooPersist.get_by(uuid=foo_key)
#        from util.log import logger
#        logger.error(foo_persist.enum_set_field)
#        self.assertEqual(2, len(foo_persist.enum_set_field))
#        enum_results = get_cassandra_all('b_EnumSet', 'number')
#        self.assertEqual(2, len(enum_results))
#        self.assertTrue(0 in enum_results)
#        self.assertTrue(2 in enum_results)

        foo_persist = FooPersist.get_by(uuid=foo_key)
        self.assertEqual(u'strabcd', foo_persist.str_field)
        self.assertEqual(u'uni & abcd', foo_persist.uni_field)
        self.assertEqual(FooEnum[u'grob'], foo_persist.enum_field)
        self.assertEqual(22, foo_persist.int_field)
        self.assertEqual(50000000000L, foo_persist.long_field)
        self.assertEqual(3.14159265, foo_persist.float_field)
        self.assertTrue(foo_persist.boolean_field)
        self.assertEqual(None, foo_persist.baz_many)
        self.assertEqual(None, foo_persist.baz_one)
        self.assertEqual(date(1998, 12, 31), foo_persist.date_field)
        self.assertEqual(datetime(2001, 5, 17, 22, 58, 59), foo_persist.datetime_field)
        self.assertEqual(time(20, 58, 59), foo_persist.time_field)

        foo.load(FooLoader)
        self.assertEqual(u'strabcd', foo.str_field)
        self.assertEqual(u'uni & abcd', foo.uni_field)
        self.assertEqual(FooEnum[u'grob'], foo.enum_field)
        self.assertEqual(22, foo.int_field)
        self.assertEqual(50000000000L, foo.long_field)
        self.assertEqual(3.14159265, foo.float_field)
        self.assertTrue(foo.boolean_field)
        self.assertEqual(None, foo.baz_many)
        self.assertEqual(None, foo.baz_one)
        self.assertEqual(date(1998, 12, 31), foo.date_field)
        self.assertEqual(datetime(2001, 5, 17, 22, 58, 59), foo.datetime_field)
        self.assertEqual(time(20, 58, 59), foo.time_field)

        # Now let's try with Post and see what happens when you attempt
        # to assign a composite key key to None after it has been assigned.
        # I think that shouldn't be allowed, and should raise a useful error.
        new_active_session()
        post = Post.create()
        post_key = post.key
        user = User.create()
        user_key = user.key
        post.user = user
        post.posted_on = datetime(2003, 5, 17, 22, 58, 59)

        save()

        loader = StreamLoader(user)
        loader.load()
        result = loader.result
        self.assertEqual(1, len(result))

        new_active_session()

        post = Post(post_key)
        post.load(PostLoader)
        raised = False
        try:
            post.posted_on = None
        except AssignmentNotAllowed:
            raised = True
        self.assertTrue(raised)

    def test_time_uuid(self):
        """Time UUID should be a TimeUUID in Cassandra."""
        self.reset_cassandra([Time])

        time_entity_1 = Time.create()
        time_entity_1_key = time_entity_1.uuid
        time_entity_1.text = u'abc'
        time_entity_2 = Time.create()
        time_entity_2_key = time_entity_2.uuid
        time_entity_2.text = u'abc'
        time_entity_3 = Time.create()
        time_entity_3_key = time_entity_3.uuid
        time_entity_3.text = u'abc'
        time_entity_4 = Time.create()
        time_entity_4_key = time_entity_4.uuid
        time_entity_4.text = u'abc'

        # Note: this won't work.  CircularDependencyError.
        #time_entity_1.many_to_one = time_entity_1
        time_entity_2.many_to_one = time_entity_1
        time_entity_3.many_to_one = time_entity_4

        save()

        result = TimeTextLoader(u'abc').load()

        self.assertEqual(4, len(result))
        self.assertEqual(result[0].uuid, time_entity_4_key)
        self.assertEqual(result[1].uuid, time_entity_3_key)
        self.assertEqual(result[2].uuid, time_entity_2_key)
        self.assertEqual(result[3].uuid, time_entity_1_key)

        new_active_session()

        result = TimeTextLoader(u'abc', desc=False).load()

        self.assertEqual(4, len(result))
        self.assertEqual(result[0].uuid, time_entity_1_key)
        self.assertEqual(result[1].uuid, time_entity_2_key)
        self.assertEqual(result[2].uuid, time_entity_3_key)
        self.assertEqual(result[3].uuid, time_entity_4_key)

        new_active_session()

        result = TimeTextLoader(u'abc', start=time_entity_3_key).load()

        self.assertEqual(3, len(result))
        self.assertEqual(result[0].uuid, time_entity_3_key)
        self.assertEqual(result[1].uuid, time_entity_2_key)
        self.assertEqual(result[2].uuid, time_entity_1_key)

        new_active_session()

        result = TimeTextLoader(u'abc', start=time_entity_3_key, desc=False).load()

        self.assertEqual(2, len(result))
        self.assertEqual(result[0].uuid, time_entity_3_key)
        self.assertEqual(result[1].uuid, time_entity_4_key)

    def test_delete(self):
        """Delete from Cassandra."""
        self.reset_cassandra([User, Post])
        user = User.create()
        user_key = user.key
        post = Post.create()
        post_key = post.key
        post_text_value = u'\xe6\u2264\u2265'
        post.text = post_text_value
        post.posted_on = datetime(2003, 5, 17, 22, 58, 59)
        post.user = user

        save()

        # A brief aside, we are going to set up a serialized update and
        # hold on to it.
        post = Post(post_key)
        post.load(PostLoader)
        post_other_text_value = u'\xf7\xac\xae'
        post.other_text = post_other_text_value
        from backend.environments.cassandra import Cassandra
        c = Cassandra()
        serialized_update = c.get_serializable_update(post.environment, {})
        new_active_session()
        # With the new session here the above change is cleared out, but we
        # saved the serialization for testing race conditions later.

        post = Post(post_key)
        user = User(user_key)
        post.load(PostLoader)
        self.assertEqual(post.posted_on, datetime(2003, 5, 17, 22, 58, 59))
        self.assertEqual(post.user, user)
        self.assertEqual(post.kind, PostEnum[0])
#        self.assertEqual(post.kind_with_default, PostEnum[2])

        loader = StreamLoader(user_key)
        loader.load()
        result = loader.result

        self.assertEqual(1, len(result))
        self.assertEqual(post_key, result[0].key)
        self.assertEqual(post.user, user)

        post = Post(post_key)
        loader = AlphaLoader([post_text_value, PostEnum[0]])
        loader.load()
        result = loader.result
        self.assertEqual(post_key, result[0].key)

        post.environment.promote
        post.environment.promote(post, 'comments', [])

        # This is failing because post.comments is not known and with it
        # being unknown we don't know if we need to fix ...
        # this is because..
#Traceback (most recent call last):
#  File "/Users/lifto/Documents/Development/chill/hsn/backend/tests/test_cassandra.py", line 581, in test_delete
#    post.delete()
        post.delete()
#  File "/Users/lifto/Documents/Development/chill/hsn/backend/environments/core.py", line 397, in delete
#    self.environment.delete(self)
# xxx just calls delete on session with post.

#  File "/Users/lifto/Documents/Development/chill/hsn/backend/environments/session.py", line 179, in delete
#    super(Session, self).delete(entity)
# xxx interesting, session is calling delete on its base class, Memory.

#  File "/Users/lifto/Documents/Development/chill/hsn/backend/environments/memory.py", line 627, in delete
#    create_write_only=False)
# xxx memory is trying to remove this Entity from reverse relationships.
# the code goes looking for post.comments because it is a one-to-many, and we
# either need to know that there aren't any or we will need to fix this
# relationship on the other side (by setting it to None as this post is being
# deleted.)

#  File "/Users/lifto/Documents/Development/chill/hsn/backend/environments/session.py", line 376, in get_attribute
#    raise AttributeNotLoaded("Attribute Not Loaded: (%s, %s)" % (entity, attribute.name))
#AttributeNotLoaded: Attribute Not Loaded: (Post(u'2990b5a7-2834-41a6-9863-81c0aa18216a'), comments)

        self.assertEqual(post.user, user)

        save()

        loader = StreamLoader(user_key)
        loader.load()
        result = loader.result

        self.assertEqual(0, len(result))

        loader = AlphaLoader([post_text_value, PostEnum[0]])
        loader.load()
        result = loader.result

        self.assertEqual(0, len(result))

        post = Post(post_key)
        raised = False
        try:
            post.load(PostLoader)
        except NullEnvironmentError:
            raised = True
        self.assertTrue(raised)

        # What would happen if that serialized update from above were sent out,
        # would we resurrect some tables?
        # Answer -- yes, yes it does.  So you have to not do that.
#        # THESE TESTS COMMENTED OUT BECAUSE THEY FAIL.
#        c = Cassandra()
#        args = serialized_update[1]
#        kwargs = serialized_update[2]
#        c.run_serialized_update(*args, **kwargs)
#
#        new_active_session()
#        post = Post(post_key)
#        raised = False
#        try:
#            post.load(PostLoader)
#        except NullEnvironmentError:
#            raised = True
#        self.assertTrue(raised)
#
#        loader = StreamLoader(user_key)
#        loader.load()
#        result = loader.result
#
#        self.assertEqual(0, len(result))
#
#        loader = AlphaLoader([post_text_value, 0])
#        loader.load()
#        result = loader.result
#
#        self.assertEqual(0, len(result))
#
#        loader = AlphaLoader([post_other_text_value, 0])
#        loader.load()
#        result = loader.result
#
#        self.assertEqual(0, len(result))

    def test_exists(self):
        """Test Exists."""
        self.reset_cassandra([Foo])
        fake_foo_key = generate_id()
        self.assertFalse(Foo(fake_foo_key).exists)

        foo = Foo.create()
        foo_key = foo.key

        save()

        self.assertTrue(Foo(foo_key).exists)
        self.assertFalse(Foo(fake_foo_key).exists)

    def test_miss(self):
        """Load a miss."""
        self.reset_cassandra([Foo, Baz])
        foo_key = generate_id()
        try:
            FooLoader(foo_key).load()
        except NullEnvironmentError:
            self.assertTrue(True)
        else:
            self.assertTrue(False)
        new_active_session()

        loader = FooLoader(foo_key)
        loader.load(suppress_misses=True)
        foo = Foo(foo_key)
        self.assertEqual(1, len(foo.failed_loaders))
        self.assertTrue(loader in foo.failed_loaders)

        new_active_session()
        foo = Foo.create()
        foo_key = foo.key
        save()

        # Make a fake baz, (suppress write to SQL), so when we attempt to
        # load it the failue will be on the Baz, which is nested in the
        # FooLoader inventory.
        baz_key = generate_id()
        baz = Baz(baz_key)
        foo = Foo(foo_key)
        foo.baz_one = baz
        get_active_session().save(try_later_on_this_thread_first=True,
                                  main_persistent=False)
        new_active_session()

        # Now we have a Foo record in Cassandra that holds a key for a bogus
        # Baz.  When we load it won't be able to find the Baz record.
        foo = Foo(foo_key)
        baz = Baz(baz_key)
        loader = FooLoader(foo_key)
        loader.load(suppress_misses=True)

        self.assertTrue(loader.load_failed)
        self.assertEqual(1, len(foo.failed_loaders))
        self.assertTrue(loader in foo.failed_loaders)
        self.assertTrue(foo.load_failed)
        self.assertEqual(0, len(baz.failed_loaders))
        self.assertFalse(baz.load_failed)

    # xxx this is disabled because DSE Cassandra does not like some of our
    # consistency levels like 'ALL' and 'EACH_QUORUM'.  When that issue is
    # resolved, reinstate this text.
    def xxx_consistency_args(self):
        """Consistency args."""
        self.reset_cassandra()

        # Create Persistent object.
        foo = Foo.create()
        foo_key = foo.key

        # should force all consistency to be 'quorum'
        save()

        foo2 = Foo.create()
        foo2_key = foo2.key
        bar = Bar.create()
        bar_key = bar.key

        # Should have consistency of settings.BACKEND_WRITE_CONSISTENCY
        # except for Bar, which has its own consistency in the schema.
        save()

        foo = Foo(foo_key)
        self.assertEqual(foo_key, get_cassandra(foo, 'uuid'))
# xxx we need to fix the Nones are blanks issue, this should be None!
#        self.assertEqual(None, get_cassandra(foo, 'str_field'))
        self.assertEqual('', get_cassandra(foo, 'str_field'))
        self.assertEqual(u'', get_cassandra(foo, 'uni_field'))
        self.assertEqual(0, get_cassandra(foo, 'enum_field'))
        self.assertEqual(0, get_cassandra(foo, 'int_field'))
        self.assertEqual(0, get_cassandra(foo, 'long_field'))
        self.assertEqual(0.0, get_cassandra(foo, 'float_field'))
        self.assertFalse(get_cassandra(foo, 'boolean_field'))
        self.assertEqual(None, get_cassandra(foo, 'baz_many'))
        self.assertEqual(None, get_cassandra(foo, 'baz_one'))
        self.assertEqual(0, get_cassandra_raw('b_Foo__counts', 'inc_field', 'uuid', foo_key))
        self.assertEqual(None, get_cassandra(foo, 'date_field'))
        self.assertEqual(None, get_cassandra(foo, 'datetime_field'))
        self.assertEqual(None, get_cassandra(foo, 'time_field'))

        foo.load([FooLoader, FooSmallLoader, FooOtherSmallLoader])

        self.assertEqual(foo_key, foo.key)
        self.assertEqual(foo_key, foo.uuid)
        self.assertEqual(u'', foo.str_field)
        self.assertEqual(u'', foo.uni_field)
        self.assertEqual(FooEnum[0], foo.enum_field)
        self.assertEqual(0, foo.int_field)
        self.assertEqual(0, foo.long_field)
        self.assertEqual(0.0, foo.float_field)
        self.assertFalse(foo.boolean_field)
        self.assertEqual(None, foo.baz_many)
        self.assertEqual(None, foo.baz_one)
        self.assertEqual(0, foo.inc_field)
        self.assertEqual(None, foo.date_field)
        self.assertEqual(None, foo.datetime_field)
        self.assertEqual(None, foo.time_field)

        new_active_session()
        foo = Foo(foo_key)
        foo.load([FooLoader, FooSmallLoader, FooOtherSmallLoader],
            consistency='ONE')

        self.assertEqual(foo_key, foo.key)
        self.assertEqual(foo_key, foo.uuid)
        self.assertEqual(u'', foo.str_field)
        self.assertEqual(u'', foo.uni_field)
        self.assertEqual(FooEnum[0], foo.enum_field)
        self.assertEqual(0, foo.int_field)
        self.assertEqual(0, foo.long_field)
        self.assertEqual(0.0, foo.float_field)
        self.assertFalse(foo.boolean_field)
        self.assertEqual(None, foo.baz_many)
        self.assertEqual(None, foo.baz_one)
        self.assertEqual(0, foo.inc_field)
        self.assertEqual(None, foo.date_field)
        self.assertEqual(None, foo.datetime_field)
        self.assertEqual(None, foo.time_field)

        new_active_session()
        foo = Foo(foo_key)
        foo.load([FooLoader])

        self.assertEqual(foo_key, foo.key)
        self.assertEqual(foo_key, foo.uuid)
        self.assertEqual(u'', foo.str_field)
        self.assertEqual(u'', foo.uni_field)
        self.assertEqual(FooEnum[0], foo.enum_field)
        self.assertEqual(0, foo.int_field)
        self.assertEqual(0, foo.long_field)
        self.assertEqual(0.0, foo.float_field)
        self.assertFalse(foo.boolean_field)
        self.assertEqual(None, foo.baz_many)
        self.assertEqual(None, foo.baz_one)
        self.assertEqual(0, foo.inc_field)
        self.assertEqual(None, foo.date_field)
        self.assertEqual(None, foo.datetime_field)
        self.assertEqual(None, foo.time_field)

    # xxx this test is disabled because it uses a loader with a non 'QUORUM'
    # consistency.  When the issue with DSE consistency is fixed, reinstate
    # this test.
    def xxx_defaults(self):
        """Default create in Cassandra"""
        self.reset_cassandra()

        # Create Persistent object.
        foo = Foo.create()
        foo_key = foo.key
        self.assertEqual(None, foo.str_field)

        save()

        foo = Foo(foo_key)
        self.assertEqual(foo_key, get_cassandra(foo, 'uuid'))
# xxx we need to fix the Nones are blanks issue, this should be None!
#        self.assertEqual(None, get_cassandra(foo, 'str_field'))
        self.assertEqual('', get_cassandra(foo, 'str_field'))
        self.assertEqual(u'', get_cassandra(foo, 'uni_field'))
        self.assertEqual(0, get_cassandra(foo, 'enum_field'))
        self.assertEqual(0, get_cassandra(foo, 'int_field'))
        self.assertEqual(0, get_cassandra(foo, 'long_field'))
        self.assertEqual(0.0, get_cassandra(foo, 'float_field'))
        self.assertFalse(get_cassandra(foo, 'boolean_field'))
        self.assertEqual(None, get_cassandra(foo, 'baz_many'))
        self.assertEqual(None, get_cassandra(foo, 'baz_one'))
        self.assertEqual(0, get_cassandra_raw('b_Foo__counts', 'inc_field', 'uuid', foo_key))
        self.assertEqual(None, get_cassandra(foo, 'date_field'))
        self.assertEqual(None, get_cassandra(foo, 'datetime_field'))
        self.assertEqual(None, get_cassandra(foo, 'time_field'))

        foo.load([FooLoader, FooSmallLoader, FooOtherSmallLoader])

        self.assertEqual(foo_key, foo.key)
        self.assertEqual(foo_key, foo.uuid)
        self.assertEqual(u'', foo.str_field)
        self.assertEqual(u'', foo.uni_field)
        self.assertEqual(FooEnum[0], foo.enum_field)
        self.assertEqual(0, foo.int_field)
        self.assertEqual(0, foo.long_field)
        self.assertEqual(0.0, foo.float_field)
        self.assertFalse(foo.boolean_field)
        self.assertEqual(None, foo.baz_many)
        self.assertEqual(None, foo.baz_one)
        self.assertEqual(0, foo.inc_field)
        self.assertEqual(None, foo.date_field)
        self.assertEqual(None, foo.datetime_field)
        self.assertEqual(None, foo.time_field)

        baz = Baz.create()
        baz_key = baz.key
        foo = Foo(foo_key)
        foo.baz_many = baz

        save()

        baz = Baz(baz_key)
        baz.load(BazLoader)
        self.assertEqual(baz_key, get_cassandra(baz, 'uuid'))
        self.assertEqual(0, get_cassandra(baz, 'int_field'))
        self.assertEqual(u'', get_cassandra(baz, 'uni_field'))
        self.assertEqual(baz_key, baz.key)
        self.assertEqual(baz_key, baz.uuid)
        self.assertEqual(0, baz.int_field)
        self.assertEqual(u'', baz.uni_field)
        foo = Foo(foo_key)
        foo.load(FooLoader)
        self.assertEqual(baz_key, get_cassandra(foo, 'baz_many'))
        self.assertEqual(baz, foo.baz_many)
        self.assertEqual(None, foo.baz_one)

        new_active_session()
        foo = Foo(foo_key)
        self.assertEqual(baz_key, get_cassandra(foo, 'baz_many'))
        foo.load(FooLoader)
        self.assertEqual(baz, foo.baz_many)
        self.assertEqual(None, foo.baz_one)
        self.assertEqual(0, baz.int_field)
        self.assertEqual(u'', baz.uni_field)

        foo.baz_many = None

        save()

        foo = Foo(foo_key)
        foo.load(FooLoader)
        self.assertEqual(None, get_cassandra(foo, 'baz_many'))
        self.assertEqual(None, foo.baz_many)

        baz = Baz(baz_key)
        foo.baz_many = baz

        save()

        baz = Baz(baz_key)
        baz.load(BazLoader)
        self.assertEqual(baz_key, get_cassandra(baz, 'uuid'))

        foo = Foo(foo_key)
        foo.load(FooLoader)
        self.assertEqual(baz_key, get_cassandra(foo, 'baz_many'))
        self.assertEqual(baz, foo.baz_many)

    def test_no_defaults_non_sync_create(self):
        """No default create in Cassandra with non sync created relationship objects."""
        self.reset_cassandra([Foo, EnumSet, Baz])

        # Create Persistent object.
        foo = Foo.create()
        foo_key = foo.key
        foo.str_field = 'strabcd'
        foo.uni_field = u'uni & abcd'
        foo.int_field = 22
        foo.long_field = 50000000000L
        foo.increment('inc_field', 15)
        foo.float_field = 3.14159265
        foo.enum_field = FooEnum[u'grob']
        foo._enum_set_field = [FooEnum[u'frob'], FooEnum[u'blob']]
        foo.boolean_field = True
        foo.date_field = date(1998, 12, 31)
        foo.datetime_field = datetime(2001, 5, 17, 22, 58, 59)
        foo.time_field = time(20, 58, 59)

        save()

        foo = Foo(foo_key)
        self.assertEqual(foo_key, get_cassandra(foo, 'uuid'))
        self.assertEqual(u'strabcd', get_cassandra(foo, 'str_field'))
        self.assertEqual(u'uni & abcd', get_cassandra(foo, 'uni_field'))
        self.assertEqual(FooEnum[u'grob'].number,
                         get_cassandra(foo, 'enum_field'))
        self.assertEqual(22, get_cassandra(foo, 'int_field'))
        self.assertEqual(50000000000L, get_cassandra(foo, 'long_field'))
        self.assertEqual(3.14159265, get_cassandra(foo, 'float_field'))
        self.assertTrue(get_cassandra(foo, 'boolean_field'))
        self.assertEqual(None, get_cassandra(foo, 'baz_many'))
        self.assertEqual(None, get_cassandra(foo, 'baz_one'))
        enum_results = get_cassandra_all('b_EnumSet', 'number')
        self.assertEqual(2, len(enum_results))
        self.assertTrue(0 in enum_results)
        self.assertTrue(2 in enum_results)
        self.assertEqual(15, get_cassandra_raw('b_Foo__counts', 'inc_field', 'uuid', foo_key))
        self.assertEqual(to_hsndate(date(1998, 12, 31)), get_cassandra(foo, 'date_field'))
        self.assertEqual(to_hsntime(datetime(2001, 5, 17, 22, 58, 59)), get_cassandra(foo, 'datetime_field'))
        self.assertEqual(to_timestorage(time(20, 58, 59)), get_cassandra(foo, 'time_field'))

        foo.load(FooLoader)
        self.assertEqual(u'strabcd', foo.str_field)
        self.assertEqual(u'uni & abcd', foo.uni_field)
        self.assertEqual(FooEnum[u'grob'], foo.enum_field)
        self.assertEqual(22, foo.int_field)
        self.assertEqual(50000000000L, foo.long_field)
        self.assertEqual(3.14159265, foo.float_field)
        self.assertTrue(foo.boolean_field)
        self.assertEqual(None, foo.baz_many)
        self.assertEqual(None, foo.baz_one)
#        enum_results = get_cassandra_all('b_EnumSet', 'number')
#        self.assertEqual(2, len(enum_results))
#        self.assertTrue(0 in enum_results)
#        self.assertTrue(2 in enum_results)
        self.assertEqual(15, foo.inc_field)
        self.assertEqual(date(1998, 12, 31), foo.date_field)
        self.assertEqual(datetime(2001, 5, 17, 22, 58, 59), foo.datetime_field)
        self.assertEqual(time(20, 58, 59), foo.time_field)

# not sure if this is supported.
#        foo.enum_set_field.add(FooEnum[u'grob'])
#        save()

        baz = Baz.create()
        baz_key = baz.key
        baz.int_field = 22
        baz.uni_field = u'aboe&*('
        foo = Foo(foo_key)
        foo.baz_many = baz
        foo.baz_one = baz
        foo.date_field = date(1999, 11, 25)
        foo.datetime_field = datetime(200, 6, 14, 21, 2, 3)
        foo.time_field = time(19, 57, 58)

        save()

        baz = Baz(baz_key)
        self.assertEqual(baz_key, get_cassandra(baz, 'uuid'))
        self.assertEqual(22, get_cassandra(baz, 'int_field'))
        self.assertEqual(u'aboe&*(', get_cassandra(baz, 'uni_field'))
        baz.load(BazLoader)
        self.assertEqual(22, baz.int_field)
        self.assertEqual(u'aboe&*(', baz.uni_field)
        foo = Foo(foo_key)
        self.assertEqual(baz_key, get_cassandra(foo, 'baz_many'))
        self.assertEqual(baz_key, get_cassandra(foo, 'baz_one'))
        self.assertEqual(to_hsndate(date(1999, 11, 25)), get_cassandra(foo, 'date_field'))
        self.assertEqual(to_hsntime(datetime(200, 6, 14, 21, 2, 3)), get_cassandra(foo, 'datetime_field'))
        self.assertEqual(to_timestorage(time(19, 57, 58)), get_cassandra(foo, 'time_field'))
        foo.load(FooLoader)
        self.assertEqual(baz, foo.baz_many)
        self.assertEqual(baz, foo.baz_one)
        self.assertEqual(date(1999, 11, 25), foo.date_field)
        self.assertEqual(datetime(200, 6, 14, 21, 2, 3), foo.datetime_field)
        self.assertEqual(time(19, 57, 58), foo.time_field)

        foo.baz_many = None
        foo.baz_one = None

        save()

        foo = Foo(foo_key)
        self.assertEqual(None, get_cassandra(foo, 'baz_many'))
        self.assertEqual(None, get_cassandra(foo, 'baz_one'))
        foo.load(FooLoader)
        self.assertEqual(None, foo.baz_many)
        self.assertEqual(None, foo.baz_one)

        baz = Baz(baz_key)
        self.assertEqual(22, get_cassandra(baz, 'int_field'))
        self.assertEqual(u'aboe&*(', get_cassandra(baz, 'uni_field'))
        baz.load(BazLoader)
        self.assertEqual(22, baz.int_field)
        self.assertEqual(u'aboe&*(', baz.uni_field)
        foo.baz_many = baz
        foo.baz_one = baz

        save()

        baz = Baz(baz_key)
        self.assertEqual(baz_key, get_cassandra(baz, 'uuid'))

        foo = Foo(foo_key)
        self.assertEqual(baz_key, get_cassandra(foo, 'baz_many'))
        self.assertEqual(baz_key, get_cassandra(foo, 'baz_one'))
        foo.load(FooLoader)
        self.assertEqual(baz, foo.baz_many)
        self.assertEqual(baz, foo.baz_one)

    def test_no_defaults_sync_create(self):
        """No default create in Cassandra with sync created relationship objects."""
        self.reset_cassandra([Foo, Baz])

        # Create Persistent object.
        foo = Foo.create()
        foo_key = foo.key
        baz = Baz.create()
        baz_key = baz.key
        baz.int_field = 22
        baz.uni_field = u'aboe&*('
        foo.str_field = 'strabcd'
        foo.uni_field = u'uni & abcd'
        foo.int_field = 22
        foo.long_field = 50000000000L
        foo.increment('inc_field', 15)
        foo.float_field = 3.14159265
        foo.enum_field = FooEnum[u'grob']
        foo.boolean_field = True
        foo.baz_many = baz
        foo.baz_one = baz
        foo.date_field = date(1998, 12, 31)
        foo.datetime_field = datetime(2001, 5, 17, 22, 58, 59)
        foo.time_field = time(20, 58, 59)

        save()

        foo = Foo(foo_key)
        baz = Baz(baz_key)
        self.assertEqual(foo_key, get_cassandra(foo, 'uuid'))
        self.assertEqual(u'strabcd', get_cassandra(foo, 'str_field'))
        self.assertEqual(u'uni & abcd', get_cassandra(foo, 'uni_field'))
        self.assertEqual(FooEnum[u'grob'].number,
                         get_cassandra(foo, 'enum_field'))
        self.assertEqual(22, get_cassandra(foo, 'int_field'))
        self.assertEqual(50000000000L, get_cassandra(foo, 'long_field'))
        self.assertEqual(3.14159265, get_cassandra(foo, 'float_field'))
        self.assertTrue(get_cassandra(foo, 'boolean_field'))
        self.assertEqual(baz_key, get_cassandra(foo, 'baz_many'))
        self.assertEqual(baz_key, get_cassandra(foo, 'baz_one'))
        self.assertEqual(15, get_cassandra_raw('b_Foo__counts', 'inc_field', 'uuid', foo_key))
        self.assertEqual(to_hsndate(date(1998, 12, 31)), get_cassandra(foo, 'date_field'))
        self.assertEqual(to_hsntime(datetime(2001, 5, 17, 22, 58, 59)), get_cassandra(foo, 'datetime_field'))
        self.assertEqual(to_timestorage(time(20, 58, 59)), get_cassandra(foo, 'time_field'))

        foo.baz_many = None
        foo.baz_one = None
        foo.date_field = date(1999, 11, 25)
        foo.datetime_field = datetime(200, 6, 14, 21, 2, 3)
        foo.time_field = time(19, 57, 58)

        save()

        foo = Foo(foo_key)
        baz = Baz(baz_key)
        self.assertEqual(None, get_cassandra(foo, 'baz_many'))
        self.assertEqual(None, get_cassandra(foo, 'baz_one'))
        self.assertEqual(to_hsndate(date(1999, 11, 25)), get_cassandra(foo, 'date_field'))
        self.assertEqual(to_hsntime(datetime(200, 6, 14, 21, 2, 3)), get_cassandra(foo, 'datetime_field'))
        self.assertEqual(to_timestorage(time(19, 57, 58)), get_cassandra(foo, 'time_field'))

        foo.baz_many = baz
        foo.baz_one = baz

        save()

        foo = Foo(foo_key)
        baz = Baz(baz_key)
        self.assertEqual(baz_key, get_cassandra(baz, 'uuid'))
        self.assertEqual(baz_key, get_cassandra(foo, 'baz_many'))
        self.assertEqual(baz_key, get_cassandra(foo, 'baz_one'))

    def test_increment_none_default(self):
        """None-default increment in Cassandra"""
        self.reset_cassandra([IncNone])

        # Create Persistent object.
        inc = IncNone.create()
        inc_key = inc.key

        self.assertEqual(inc.inc1, None)
        self.assertEqual(inc.inc2, None)

        save()

        inc = IncNone(inc_key)
        raised = False
        try:
            inc.inc1 = 22
        except TypeError:
            raised = True
        self.assertTrue(raised)
        inc.load(IncNone1Loader)
        # xxx TAKE THIS OUT UNTIL MIGRATION OF INCREMENTS IS DONE - MEC hack
        self.assertEqual(inc.inc1, 0)#None)
        inc.load(IncNone2Loader)
        self.assertEqual(inc.inc2, 0)#None)

        new_active_session()

        # Try an increment without first loading.
        inc = IncNone(inc_key)
        inc.increment('inc1', 3)
        inc.increment('inc2', 5)

        save()

        inc = IncNone(inc_key)
        # Test presence of values in Cassandra without using a Loader.
        self.assertEqual(inc_key, get_cassandra(inc, 'uuid'))
        self.assertEqual(3, get_cassandra_raw('b_IncNone__counts', 'inc1', 'uuid', inc_key))
        self.assertEqual(5, get_cassandra_raw('b_IncNone__counts', 'inc2', 'uuid', inc_key))

        # See if the Loaders work as expected.
        inc.load(IncNone1Loader)
        self.assertEqual(inc.inc1, 3)
        raised = False
        try:
            inc.inc2
        except AttributeNotLoaded:
            raised = True
        self.assertTrue(raised)

        inc.load(IncNone2Loader)
        self.assertEqual(inc.inc2, 5)

        new_active_session()

        inc = IncNone(inc_key)

        inc.load(IncNone2Loader)
        self.assertEqual(inc.inc2, 5)
        raised = False
        try:
            inc.inc1
        except AttributeNotLoaded:
            raised = True
        self.assertTrue(raised)

        new_active_session()

        # try an increment after a load.
        inc = IncNone(inc_key)
        inc.load(IncNone1Loader)

        inc.increment('inc1', 2)

        self.assertEqual(inc.inc1, 5)

        save()

        inc = IncNone(inc_key)
        inc.load(IncNone1Loader)

        self.assertEqual(inc.inc1, 5)

        # try an increment before a load.
        new_active_session()

        inc = IncNone(inc_key)
        inc.increment('inc1', 6)
        inc.load(IncNone1Loader)

        self.assertEqual(inc.inc1, 11)

        save()

        # The loaders seem to work.  Make a second IncNone, this time setting
        # the initial values (instead of just using the default initial
        # values.)
        # note you can't set initial values, you must increment and account
        # for the default.

        inc2 = IncNone.create()
        inc2_key = inc2.key
        inc2.increment('inc1', 50)
        inc2.increment('inc2', 90)

        self.assertEqual(inc2.inc1, 50)
        self.assertEqual(inc2.inc2, 90)

        save()

        # check the presence of the values in Cassandra.
        inc2 = IncNone(inc2_key)
        self.assertEqual(inc2_key, get_cassandra(inc2, 'uuid'))
        self.assertEqual(50, get_cassandra_raw('b_IncNone__counts', 'inc1', 'uuid', inc2_key))
        self.assertEqual(90, get_cassandra_raw('b_IncNone__counts', 'inc2', 'uuid', inc2_key))

        # Increment the fields without a load.
        inc2.increment('inc1', 3)
        inc2.increment('inc2', 5)

        save()

        self.assertEqual(53, get_cassandra_raw('b_IncNone__counts', 'inc1', 'uuid', inc2_key))
        self.assertEqual(95, get_cassandra_raw('b_IncNone__counts', 'inc2', 'uuid', inc2_key))

        # Create a new object, set non-default values, and increment before
        # saving.

        inc3 = IncNone.create()
        inc3_key = inc3.key
        inc3.increment('inc1', 20)
        inc3.increment('inc2', 290)
        inc3.increment('inc1', 44)
        inc3.increment('inc2', 5000)

        save()

        inc3 = IncNone(inc3_key)
        self.assertEqual(64, get_cassandra_raw('b_IncNone__counts', 'inc1', 'uuid', inc3_key))
        self.assertEqual(5290, get_cassandra_raw('b_IncNone__counts', 'inc2', 'uuid', inc3_key))

    def test_increment(self):
        """Increment in Cassandra"""
        self.reset_cassandra([Foo])

        # Create Persistent object.
        foo = Foo.create()
        foo_key = foo.key

        self.assertEqual(foo.inc_field, 0)
        self.assertEqual(foo.inc2_field, 10)

        save()

        foo = Foo(foo_key)
        foo.load(FooLoader)
        self.assertEqual(foo.inc_field, 0)
        foo.load(FooCountLoader)
        self.assertEqual(foo.inc2_field, 10)

        new_active_session()

        # Try an increment without first loading.
        foo = Foo(foo_key)
        foo.increment('inc_field', 3)
        foo.increment('inc2_field', 5)

        save()

        foo = Foo(foo_key)
        # Test presence of values in Cassandra without using a Loader.
        self.assertEqual(foo_key, get_cassandra(foo, 'uuid'))
        self.assertEqual(3, get_cassandra_raw('b_Foo__counts', 'inc_field', 'uuid', foo_key))
        self.assertEqual(15, get_cassandra_raw('b_Foo__counts', 'inc2_field', 'uuid', foo_key))

        # See if the Loaders work as expected.
        foo.load(FooLoader)
        self.assertEqual(foo.inc_field, 3)
        raised = False
        try:
            foo.inc2_field
        except AttributeNotLoaded:
            raised = True
        self.assertTrue(raised)

        foo.load(FooCountLoader)
        self.assertEqual(foo.inc2_field, 15)

        new_active_session()

        foo = Foo(foo_key)

        foo.load(FooCountLoader)
        self.assertEqual(foo.inc2_field, 15)
        raised = False
        try:
            foo.inc_field
        except AttributeNotLoaded:
            raised = True
        self.assertTrue(raised)

        new_active_session()

        # try an increment after a load.
        foo = Foo(foo_key)
        foo.load(FooLoader)

        foo.increment('inc_field', 2)

        self.assertEqual(foo.inc_field, 5)

        save()

        foo = Foo(foo_key)
        foo.load(FooLoader)

        self.assertEqual(foo.inc_field, 5)

        # try an increment before a load.
        new_active_session()

        foo = Foo(foo_key)
        foo.increment('inc_field', 6)
        foo.load(FooLoader)

        self.assertEqual(foo.inc_field, 11)

        save()

        # The loaders seem to work.  Make a second Foo, this time setting
        # the initial values (instead of just using the default initial
        # values.)
        # note you can't set initial values, you must increment and account
        # for the default.

        foo2 = Foo.create()
        foo2_key = foo2.key
        foo2.increment('inc_field', 50)
        foo2.increment('inc2_field', 90)

        self.assertEqual(foo2.inc_field, 50)
        self.assertEqual(foo2.inc2_field, 100)

        save()

        # check the presence of the values in Cassandra.
        foo2 = Foo(foo2_key)
        self.assertEqual(foo2_key, get_cassandra(foo2, 'uuid'))
        self.assertEqual(50, get_cassandra_raw('b_Foo__counts', 'inc_field', 'uuid', foo2_key))
        self.assertEqual(100, get_cassandra_raw('b_Foo__counts', 'inc2_field', 'uuid', foo2_key))

        # Increment the fields without a load.
        foo2.increment('inc_field', 3)
        foo2.increment('inc2_field', 5)

        save()

        self.assertEqual(53, get_cassandra_raw('b_Foo__counts', 'inc_field', 'uuid', foo2_key))
        self.assertEqual(105, get_cassandra_raw('b_Foo__counts', 'inc2_field', 'uuid', foo2_key))

        # Create a new object, set non-default values, and increment before
        # saving.

        foo3 = Foo.create()
        foo3_key = foo3.key
        foo3.increment('inc_field', 20)
        foo3.increment('inc2_field', 290)
        foo3.increment('inc_field', 44)
        foo3.increment('inc2_field', 5000)

        save()

        foo3 = Foo(foo3_key)
        self.assertEqual(64, get_cassandra_raw('b_Foo__counts', 'inc_field', 'uuid', foo3_key))
        self.assertEqual(5300, get_cassandra_raw('b_Foo__counts', 'inc2_field', 'uuid', foo3_key))

    def test_increment_null(self):
        """Null rows in the count table should read as Nones."""
        self.reset_cassandra([Foo])

        inc_field = Foo.get_attribute('inc_field')
        inc2_field = Foo.get_attribute('inc2_field')

        foo = Foo.create()
        foo_key = foo.key

        save()

        foo = Foo(foo_key)

        # Instead of performing a migration we will delete the entity's row
        # from the counters Column family and thus simulate the effect of a
        # migration that adds a counter Column family when there were no
        # previous IncrementFields.
        batch(deletes={'b_foo__counts': [{'uuid': foo_key}]})

        foo.load(FooBothCountLoader)

        # XXX when we remove the incr hack following successful migration
        # of existing incr nones we put this back
#        self.assertEqual(None, foo.inc_field)
#        self.assertEqual(None, foo.inc2_field)
        self.assertEqual(0, foo.inc_field)
        self.assertEqual(0, foo.inc2_field)

        fake_foo = Foo(generate_id())

        raised = None
        try:
            fake_foo.load(FooBothCountLoader)
        except NullEnvironmentError, e:
            raised = e
        self.assertEqual('NullEnvironmentError', type(raised).__name__)

        get_active_session().increment(foo, inc_field, 3)
        get_active_session().increment(foo, inc2_field, 4)

        save()

        foo = Foo(foo_key)
        foo.load(FooBothCountLoader)

        # These were deleted, and will be read as None.
        # xxx put this back following the migration of incrs nones to 0 and
        # the removal of the incrnonezero hack.
        self.assertEqual(0, foo.inc_field)
        self.assertEqual(0, foo.inc2_field)
#        self.assertEqual(None, foo.inc_field)
#        self.assertEqual(None, foo.inc2_field)

    # xxx: disabled because Bar has a custom non-quorum consistency defined.
    # when we resolve the issue where DSE rejects an 'ALL' consistency,
    # reinstate this test.
    def xxx_cache(self):
        "Cassandra should load an Inventory."
        self.reset_cassandra()

        # Create Persistent objects.
        foo = Foo.create()
        foo_key = foo.key

        bridge = Bridge.create()
        bridge_key = bridge.key

        bar = Bar.create()
        bar_key = bar.key

        # Set up the bridge, but not the bar.
        bridge.foo1 = foo
        bridge.foo2 = foo

        save()

    # xxx disabled as part of an upgrade.  replace with a test for KeyNotLoaded
#    def test_composite_empty_key_error_on_create(self):
#        """Empty keys (that are in composite keys) raise error on create."""
#        self.reset_cassandra()
#        post = Post.create()
#
#        # Queue Cassandra for a 'later' save.  One might expect that the
#        # empty key exception would only arise in the queued task, but backend
#        # checks for this error condition before it saves.
#        # Note that Cassandra is queued for 'later' by default, but this is in
#        # the test to ensure it works for 'later' even if the default changes.
#        backend.Cassandra().save_later()
#
#        # This will not fill in necessary fields, so when the backend save is
#        # called (in our web service, it is called automatically in the
#        # middleware) it will raise an exception.
#
#        raised = False
#        try:
#            get_active_session().save(try_later_on_this_thread_first=True)
#        except EmptyKeyError, e:
#            raised = True
#        self.assertTrue(raised)
#
#        drop_cassandra()
#
#    def test_composite_empty_key_error_on_update(self):
#        """Empty keys (that are in composite keys) raise error on update."""
#        self.reset_cassandra()
#        user = User.create()
#        user_key = user.key
#        post = Post.create()
#        post_key = post.key
#        post.text = 'testtext'
#        post.posted_on = datetime(2003, 5, 17, 22, 58, 59)
#        post.user = user
#
#        # Queue Cassandra for a 'later' save.  One might expect that the
#        # empty key exception would only arise in the queued task, but backend
#        # checks for this error condition before it saves.
#        # Note that Cassandra is queued for 'later' by default, but this is in
#        # the test to ensure it works for 'later' even if the default changes.
#        backend.Cassandra().save_later()
#
#        # This will not fill in necessary fields, so when the backend save is
#        # called (in our web service, it is called automatically in the
#        # middleware) it will raise an exception.
#        save()
#
#        post = Post(post_key)
#        post.other_user = User(user_key)
#
#        Cassandra().save_later()
#
#        raised = False
#        try:
#            get_active_session().save(try_later_on_this_thread_first=True)
#        except EmptyKeyError, e:
#            from util.log import logger
#            logger.error(e)
#            logger.exception(e)
#            raised = True
#        self.assertTrue(raised)
#
#        new_active_session()
#
#        post = Post(post_key)
#        post.load(PostLoader)
#        post.other_user = User(user_key)
#
#        Cassandra().save_later()
#        save()
#
#        new_active_session()
#        post = Post(post_key)
#        post.load(PostLoader)
#        self.assertEqual(post.other_user, User(user_key))
#
#        drop_cassandra()


## xxx no they may not!
#
#    def test_composite_key_changed(self):
#        """Composite keys may be changed."""
#        self.reset_cassandra()
#        user1 = User.create()
#        user1_key = user1.key
#        post = Post.create()
#        post_key = post.key
#        post.text = 'testtext'
#        post.posted_on = datetime(2003, 5, 17, 22, 58, 59)
#        post.user = user1
#
#        save()
#
#        loader = StreamLoader(user1_key)
#        loader.load()
#        result = loader.result
#
#        self.assertEqual(1, len(result))
#        self.assertEqual(post_key, result[0].key)
#
#        new_active_session()
#
#        # Let's change the post's user.
#        user2 = User.create()
#        user2_key = user2.key
#        post = Post(post_key)
#        post.load(PostLoader)
#        self.assertEqual(post.user.key, user1_key)
#        post.user = user2
#
#        save()
#
#        loader = StreamLoader(user2_key)
#        loader.load()
#        result = loader.result
#
##        post = Post(post_key)
##        post.load(PostLoader)
##        self.assertEqual(post.user.key, user2_key)
#
#        self.assertEqual(1, len(result))
#        self.assertEqual(post_key, result[0].key)
#
#        new_active_session()
#
#        loader = StreamLoader(user1_key)
#        loader.load()
#        result = loader.result
#
#        self.assertEqual(0, len(result))
#
#        drop_cassandra()
#
#    def test_indexes_create(self):
#        """Composite keys in created objects in Cassandra"""
#        self.reset_cassandra()
#
#        # These are the fields of the above-declared test Post object (not the
#        # backend Post!)
#        # note the composite keys 'stream' and 'alpha'
#        #    posted_on = DatetimeField()
#        #    text = UnicodeField()
#        #    user = ManyToOneField('UserPersist', inverse='posts')
#        #    kind = EnumField(PostEnum)
#        #    rank = IntegerField(default=0)
#        #    upvotes = IncrementField(default=0)
#        #    indexes = {
#        #        'stream': ('user', 'posted_on'),
#        #        'alpha': ('text', 'rank', 'kind')
#        #    }
#
#        # Create three Persistent objects with specific composite key values.
#        user1 = User.create()
#        user1_key = user1.key
#        user2 = User.create()
#        user2_key = user2.key
#        user3 = User.create()
#        user3_key = user3.key
#
#        post1 = Post.create()
#        post1_key = post1.key
#        post1.posted_on = datetime(2003, 5, 17, 22, 58, 59)
#        post1.text = 'zz'
#        post1.user = user1
#        post1.kind = PostEnum[u'unflagged']
#
#        post2 = Post.create()
#        post2_key = post2.key
#        post2.posted_on = datetime(2003, 5, 17, 22, 58, 59)
#        post2.text = 'cc'
#        post2.user = user2
#        post2.kind = PostEnum[u'unflagged']
#        post2.rank = 0
#
#        post3 = Post.create()
#        post3_key = post3.key
#        post3.posted_on = datetime(2002, 5, 17, 22, 58, 59)
#        post3.text = 'gg'
#        post3.user = user1
#        post3.kind = PostEnum[u'flagged']
#        post3.rank = 20
#
#        post4 = Post.create()
#        post4_key = post4.key
#        post4.posted_on = datetime(2001, 5, 17, 22, 58, 59)
#        post4.text = 'kk'
#        post4.user = user1
#        post4.kind = PostEnum[u'popular']
#        post4.rank = 79
#
#        save()
#        # (from the PostPersist declaration.)
#        # indexes = {
#        #     'stream': ('user_many', 'datetime_field'),
#        #     'misc': ('str_field', 'uuid', 'enum_field', 'int_field')
#        # }
#        # first are two examples using the 'stream' composite key and then
#        # two using the 'misc' composite key.
#
#        # Get all records that have the given user field in order of datetime,
#        # most recent to least recent
##        loader = StreamLoader(user1_key)
#        loader = PostLoader(post1_key)
#
#        from util.log import logger
## SELECT uni_field, datetime_field, uuid, user_many FROM b_Post_stream USING CONSISTENCY QUORUM WHERE user_many=:user_many
## {'user_many': UUID('6320e2b2-d9fd-4daa-b48d-8a3e67459b5d')}
#        loader.load()
#        result = loader.result
#        self.assertEqual(Post(post1_key), result)
#
#        new_active_session()
#
#        loader = StreamLoader(user1_key)
#        loader.load()
#        result = loader.result
#
#        self.assertEqual(3, len(result))
#        self.assertEqual(post1, result[0])
#        self.assertEqual(post3, result[1])
#        self.assertEqual(post4, result[2])
#
#        new_active_session()
#
#        loader = StreamLoader(user1_key, desc=False)
#        loader.load()
#        result = loader.result
#
#        self.assertEqual(3, len(result))
#        self.assertEqual(post1, result[2])
#        self.assertEqual(post3, result[1])
#        self.assertEqual(post4, result[0])
#
#        new_active_session()
#
#        # Get first 2 records that have the uuid field in order of datetime,
#        # most recent to least recent.
#        loader = StreamLoader(user1_key, count=2)
#        loader.load()
#        result = loader.result
#        self.assertEqual(2, len(result))
#        self.assertEqual(post1, result[0])
#        self.assertEqual(post3, result[1])
#
#        new_active_session()
#
#        # Get first 2 records that have the uuid field in order of datetime,
#        # most recent to least recent, starting with a given time.
#        loader = StreamLoader(user1_key,
#                              start=datetime(2001, 5, 17, 22, 58, 59),
#                              count=1)
#        loader.load()
#        result = loader.result
#        self.assertEqual(1, len(result))
#        self.assertEqual(post4, result[0])
#
#        new_active_session()
#
#        loader = StreamLoader(user2_key)
#        loader.load()
#        result = loader.result
#        self.assertEqual(1, len(result))
#        self.assertEqual(post2, result[0])
#
#        new_active_session()
#        # (2 examples using the 'alpha' composite key)
#        # 'alpha': ('text', 'uuid', 'kind', 'rank')
#
#        post5 = Post.create()
#        post5_key = post5.key
#        post5.posted_on = datetime(1982, 5, 17, 22, 58, 59)
#        post5.text = 'foobarbaz'
#        post5.user = user1
#        post5.kind = PostEnum[u'popular']
#        post5.rank = 20
#
#        post6 = Post.create()
#        post6_key = post6.key
#        post6.posted_on = datetime(1983, 5, 17, 22, 58, 59)
#        post6.text = 'foobarbaz'
#        post6.user = user1
#        post6.kind = PostEnum[u'popular']
#        post6.rank = 15
#
#        save()
#
#        # Get all records, in rank order, lowest to highest, for all records with:
#        # text = 'xyz'
#        # kind = PostEnum[2]
#        # rank = 0
#        loader = AlphaLoader(['foobarbaz', 2, 20], desc=False)
#        loader.load()
#        result = loader.result
#
#        self.assertEqual(1, len(result))
#        drop_cassandra()
#        return
#        self.assertEqual(post5, result[0])
#
#        new_active_session()
#
#        # Get all records, in rank order, lowest to highest, for all records with:
#        # text = 'xyz'
#        # kind = PostEnum[2]
#        loader = AlphaLoader(['foobarbaz', PostEnum[u'popular']], desc=False)
#        loader.load()
#        result = loader.result
#
#        self.assertEqual(2, len(result))
#        self.assertEqual(post6, result[0])
#        self.assertEqual(post5, result[1])
#
#        drop_cassandra()
# xxx you can't change composite keys
#    def test_indexes_on_update(self):
#        """Composite keys in updated objects in Cassandra"""
#        self.reset_cassandra()
#
#        # These are the fields of the above-declared test Post object (not the
#        # backend Post!)
#        # note the composite keys 'stream' and 'alpha'
#        #    posted_on = DatetimeField()
#        #    text = UnicodeField()
#        #    user = ManyToOneField('UserPersist', inverse='posts')
#        #    kind = EnumField(PostEnum)
#        #    rank = IntegerField(default=0)
#        #    upvotes = IncrementField(default=0)
#        #    indexes = {
#        #        'stream': ('user', 'posted_on'),
#        #        'alpha': ('text', 'rank', 'kind')
#        #    }
#
#        user1 = User.create()
#        user1_key = user1.key
#        user2 = User.create()
#        user2_key = user2.key
#        user3 = User.create()
#        user3_key = user3.key
#
#        post1 = Post.create()
#        post1_key = post1.key
#        post1.posted_on = datetime(2003, 5, 17, 22, 58, 59)
#        post1.text = 'zz'
#        post1.user = user1
#        post1.kind = PostEnum[u'unflagged']
#
#        post2 = Post.create()
#        post2_key = post2.key
#        post2.posted_on = datetime(2003, 5, 17, 22, 58, 59)
#        post2.text = 'cc'
#        post2.user = user2
#        post2.kind = PostEnum[u'unflagged']
#        post2.rank = 0
#
#        post3 = Post.create()
#        post3_key = post3.key
#        post3.posted_on = datetime(2002, 5, 17, 22, 58, 59)
#        post3.text = 'gg'
#        post3.user = user1
#        post3.kind = PostEnum[u'flagged']
#        post3.rank = 20
#
#        post4 = Post.create()
#        post4_key = post4.key
#        post4.posted_on = datetime(2001, 5, 17, 22, 58, 59)
#        post4.text = 'kk'
#        post4.user = user1
#        post4.kind = PostEnum[u'popular']
#        post4.rank = 79
#
#        save()
#
#        # exchange the posts' information.
#        post1 = Post(post1_key)
#        post1.load(PostLoader)
#        post1.posted_on = datetime(2002, 5, 17, 22, 58, 59)
#        post1.user = user1
#        post1.kind = PostEnum[u'flagged']
#        post1.rank = 20
#
#        post2 = Post(post2_key)
#        post2.load(PostLoader)
#        post2.posted_on = datetime(2001, 5, 17, 22, 58, 59)
#        post2.user = user1
#        post2.kind = PostEnum[u'popular']
#        post2.rank = 79
#
#        post3 = Post(post3_key)
#        post3.load(PostLoader)
#        post3.posted_on = datetime(2003, 5, 17, 22, 58, 59)
#        post3.user = user1
#        post3.kind = PostEnum[u'unflagged']
#        post3.rank = 0
#
#        post4 = Post(post4_key)
#        post4.load(PostLoader)
#        post4.posted_on = datetime(2003, 5, 17, 22, 58, 59)
#        post4.user = user2
#        post4.kind = PostEnum[u'unflagged']
#        post4.rank = 0
#
#        save()
#
#        # test the posts' information from the primary table.
#        post1 = Post(post1_key)
#        post1.load(PostLoader)
#        self.assertEqual(post1.posted_on, datetime(2002, 5, 17, 22, 58, 59))
#        self.assertEqual(post1.user, user1)
#        self.assertEqual(post1.kind, PostEnum[u'flagged'])
#        self.assertEqual(post1.rank, 20)
#
#        post2 = Post(post2_key)
#        post2.load(PostLoader)
#        self.assertEqual(post2.posted_on, datetime(2001, 5, 17, 22, 58, 59))
#        self.assertEqual(post2.user, user1)
#        self.assertEqual(post2.kind, PostEnum[u'popular'])
#        self.assertEqual(post2.rank, 79)
#
#        post3 = Post(post3_key)
#        post3.load(PostLoader)
#        self.assertEqual(post3.posted_on, datetime(2003, 5, 17, 22, 58, 59))
#        self.assertEqual(post3.user, user1)
#        self.assertEqual(post3.kind, PostEnum[u'unflagged'])
#        self.assertEqual(post3.rank, 0)
#
#        post4 = Post(post4_key)
#        post4.load(PostLoader)
#        self.assertEqual(post4.posted_on, datetime(2003, 5, 17, 22, 58, 59))
#        self.assertEqual(post4.user, user2)
#        self.assertEqual(post4.kind, PostEnum[u'unflagged'])
#        self.assertEqual(post4.rank, 0)
#
#        # Test the posts's information as found in the composite key tables.
#        new_active_session()
#        from util.log import logger
#        logger.error('post keys 1,2,3,4')
#        logger.error(post1_key)
#        logger.error(post2_key)
#        logger.error(post3_key)
#        logger.error(post4_key)
#        loader = StreamLoader(user1_key)
#        loader.load()
#        result = loader.result
#        logger.error('xxx result --------------')
#        for x in result:
#            logger.error(x)
#        for post in result:
#            if post.key == post1_key:
#                self.assertEqual(post.posted_on, datetime(2002, 5, 17, 22, 58, 59))
#                self.assertEqual(post.user, user1)
#                self.assertEqual(post.kind, PostEnum[u'flagged'])
#                self.assertEqual(post.rank, 20)
#            elif post.key == post2_key:
#                self.assertEqual(post.posted_on, datetime(2001, 5, 17, 22, 58, 59))
#                self.assertEqual(post.user, user1)
#                self.assertEqual(post.kind, PostEnum[u'popular'])
#                self.assertEqual(post.rank, 79)
#            elif post.key == post3_key:
#                self.assertEqual(post.posted_on, datetime(2003, 5, 17, 22, 58, 59))
#                self.assertEqual(post.user, user1)
#                self.assertEqual(post.kind, PostEnum[u'unflagged'])
#                self.assertEqual(post.rank, 0)
#            elif post.key == post4_key:
#                self.assertEqual(post.posted_on, datetime(2003, 5, 17, 22, 58, 59))
#                self.assertEqual(post.user, user2)
#                self.assertEqual(post.kind, PostEnum[u'unflagged'])
#                self.assertEqual(post.rank, 0)
#
#
#        # This is showing 1,3,4.  This is wrong because post4 now has user2,
#        # amonong other discrepencies.
#        # Let's see if update is working as well as create.
#        drop_cassandra()
#        return
#
#        post1 = Post(post1_key)
#        post1.load(StreamLoader)
#        self.assertEqual(post1.posted_on, datetime(2002, 5, 17, 22, 58, 59))
#        self.assertEqual(post1.user, user1)
#        self.assertEqual(post1.kind, PostEnum[u'flagged'])
#        self.assertEqual(post1.rank, 20)
#
#        post2 = Post(post2_key)
#        post2.load(PostLoader)
#        self.assertEqual(post2.posted_on, datetime(2001, 5, 17, 22, 58, 59))
#        self.assertEqual(post2.user, user1)
#        self.assertEqual(post2.kind, PostEnum[u'popular'])
#        self.assertEqual(post2.rank, 79)
#
#        post3 = Post(post3_key)
#        post3.load(PostLoader)
#        self.assertEqual(post3.posted_on, datetime(2003, 5, 17, 22, 58, 59))
#        self.assertEqual(post3.user, user1)
#        self.assertEqual(post3.kind, PostEnum[u'unflagged'])
#        self.assertEqual(post3.rank, 0)
#
#        post4 = Post(post4_key)
#        post4.load(PostLoader)
#        self.assertEqual(post4.posted_on, datetime(2003, 5, 17, 22, 58, 59))
#        self.assertEqual(post4.user, user2)
#        self.assertEqual(post4.kind, PostEnum[u'unflagged'])
#        self.assertEqual(post4.rank, 0)
#
#        drop_cassandra()
#        return
#
#        # (from the PostPersist declaration.)
#        # indexes = {
#        #     'stream': ('user_many', 'datetime_field'),
#        #     'misc': ('str_field', 'uuid', 'enum_field', 'int_field')
#        # }
#        # first are two examples using the 'stream' composite key and then
#        # two using the 'misc' composite key.
#
#        # Get all records that have the given user field in order of datetime,
#        # most recent to least recent
##        loader = StreamLoader(user1_key)
#        loader = PostLoader(post1_key)
#
#        from util.log import logger
## SELECT uni_field, datetime_field, uuid, user_many FROM b_Post_stream USING CONSISTENCY QUORUM WHERE user_many=:user_many
## {'user_many': UUID('6320e2b2-d9fd-4daa-b48d-8a3e67459b5d')}
#        loader.load()
#        result = loader.result
#        self.assertEqual(Post(post1_key), result)
#
#        new_active_session()
#
#        loader = StreamLoader(user1_key)
#        loader.load()
#        result = loader.result
#
#        self.assertEqual(3, len(result))
#        self.assertEqual(post3, result[0])
#        self.assertEqual(post1, result[1])
#        self.assertEqual(post2, result[2])
#
#        new_active_session()
#
#        loader = StreamLoader(user1_key, desc=False)
#        loader.load()
#        result = loader.result
#
#        self.assertEqual(3, len(result))
#        self.assertEqual(post3, result[2])
#        self.assertEqual(post1, result[1])
#        self.assertEqual(post2, result[0])
#
#        new_active_session()
#
#        # Get first 2 records that have the uuid field in order of datetime,
#        # most recent to least recent.
#        loader = StreamLoader(user1_key, count=2)
#        loader.load()
#        result = loader.result
#        self.assertEqual(2, len(result))
#        self.assertEqual(post3, result[0])
#        self.assertEqual(post1, result[1])
#
#        new_active_session()
#
#        # Get first 2 records that have the uuid field in order of datetime,
#        # most recent to least recent, starting with a given time.
#        loader = StreamLoader(user1_key,
#                              start=datetime(2001, 5, 17, 22, 58, 59),
#                              count=1)
#        loader.load()
#        result = loader.result
#        self.assertEqual(1, len(result))
#        self.assertEqual(post2, result[0])
#
#        new_active_session()
#
#        loader = StreamLoader(user2_key)
#        loader.load()
#        result = loader.result
#        self.assertEqual(1, len(result))
#        self.assertEqual(post4, result[0])
#
#        drop_cassandra()

    def test_composite_key_inventory(self):
        """Composite keys loads should load inventory"""
        self.reset_cassandra([User, Post])

        # These are the fields of the above-declared test Post object (not the
        # backend Post!)
        # note the composite keys 'stream' and 'alpha'
        #    posted_on = DatetimeField()
        #    text = UnicodeField()
        #    user = ManyToOneField('UserPersist', inverse='posts')
        #    kind = EnumField(PostEnum)
        #    rank = IntegerField(default=0)
        #    upvotes = IncrementField(default=0)
        #    indexes = {
        #        'stream': ('user', 'posted_on'),
        #        'alpha': ('text', 'rank', 'kind')
        #    }

        user1 = User.create()
        user1_key = user1.key
        user1.name = u'user1'
        user2 = User.create()
        user2_key = user2.key
        user2.name = u'user2'
        user3 = User.create()
        user3_key = user3.key
        user3.name = u'user3'

        post1 = Post.create()
        post1_key = post1.key
        post1.posted_on = datetime(2003, 5, 17, 22, 58, 59)
        post1.text = u'zz'
        post1.user = user1
        post1.kind = PostEnum[u'unflagged']

        post2 = Post.create()
        post2_key = post2.key
        post2.posted_on = datetime(2003, 5, 17, 22, 58, 59)
        post2.text = u'cc'
        post2.user = user2
        post2.kind = PostEnum[u'unflagged']
        post2.rank = 0

        post3 = Post.create()
        post3_key = post3.key
        post3.posted_on = datetime(2002, 5, 17, 22, 58, 59)
        post3.text = u'gg'
        post3.user = user1
        post3.kind = PostEnum[u'flagged']
        post3.rank = 20
        post3.increment('upvotes', 3)

        post4 = Post.create()
        post4_key = post4.key
        post4.posted_on = datetime(2001, 5, 17, 22, 58, 59)
        post4.text = u'kk'
        post4.user = user1
        post4.kind = PostEnum[u'popular']
        post4.rank = 79

        save()

        post1 = Post(post1_key)
        post1.increment('upvotes', 1)
        post1.load(PostLoader)

        save()

        post1 = Post(post1_key)
        post1.load(PostLoader)
        post1.increment('upvotes', 3)

        save()

        loader = StreamLoader(user1_key)
        loader.load()
        posts = loader.result
        # If the inventory doesn't load, an AttributeNotFound error would
        # arise.
        for post in posts:
            self.assertEqual(post.user.name, u'user1')
        post1 = Post(post1_key)
        post3 = Post(post3_key)
        post4 = Post(post4_key)
        self.assertEqual(post1.upvotes, 4)
        self.assertEqual(post3.upvotes, 3)
        self.assertEqual(post4.upvotes, 0)

        post4 = Post(post4_key)
        post4.load(PostLoader)

# xxx this is not supported yet it seems... do not increment then load or
# you'll get a clobber.
#        new_active_session()
#
#        post1 = Post(post1_key)
#        post1.increment('upvotes', 1)
#
#        loader = StreamLoader(user1_key)
#        loader.load()
#        posts = loader.result
#
#        self.assertEqual(post1.upvotes, 5)

    def test_assignment_errors(self):
        """Assignment errors."""
        self.reset_cassandra([Post])
        # If we attempt to set any attribute's value for a Post we should get
        # an error if any of the keys are not known (loaded, assigned or
        # promoted.)
        # attributes are:
        # uuid = UUIDField(primary_key=True)
        #posted_on = DatetimeField()
        #text = UnicodeField()
        #user = ManyToOneField('UserPersist', inverse='posts')
        #other_user = ManyToOneField('UserPersist', inverse='other_posts')
        #kind = EnumField(PostEnum)
        #rank = IntegerField(default=0)

        post = Post.create()
        post_key = post.key
        save()

        # posted_on
        post = Post(post_key)
        raised = False
        try:
            post.posted_on = datetime(2003, 5, 17, 22, 58, 59)
        except KeyNotLoaded:
            raised = True
        self.assertTrue(raised)
        new_active_session()

        # text
        post = Post(post_key)
        raised = False
        try:
            post.text = u'new post text'
        except KeyNotLoaded:
            raised = True
        self.assertTrue(raised)
        new_active_session()

        # user
        post = Post(post_key)
        raised = False
        try:
            post.user = None
        except KeyNotLoaded:
            raised = True
        self.assertTrue(raised)
        new_active_session()

        # other_user
        post = Post(post_key)
        raised = False
        try:
            post.other_user = None
        except KeyNotLoaded:
            raised = True
        self.assertTrue(raised)
        new_active_session()

        # kind
        post = Post(post_key)
        raised = False
        try:
            post.kind = PostEnum[1]
        except KeyNotLoaded:
            raised = True
        self.assertTrue(raised)
        new_active_session()

        # rank
        post = Post(post_key)
        raised = False
        try:
            post.rank = 22
        except KeyNotLoaded:
            raised = True
        self.assertTrue(raised)
        new_active_session()

    def test_composite_key_update(self):
        """Composite keys updates should write and be readable."""
        self.reset_cassandra([User, Post])

        user1 = User.create()
        user1_key = user1.key
        user1.name = u'user1'

        post1 = Post.create()
        post1_key = post1.key
        post1.posted_on = datetime(2003, 5, 17, 22, 58, 59)
        post1.text = u'testpost1'
        post1.user = user1
        post1.kind = PostEnum[u'unflagged']

        save()

        loader = StreamLoader(user1_key)
        loader.load()
        posts = loader.result
        # If the inventory doesn't load, an AttributeNotFound error would
        # arise.
        for post in posts:
            self.assertEqual(post.user.name, u'user1')
            self.assertEqual(post.other_user, None)

        post1 = Post(post1_key)
        post1.load(PostLoader)
        self.assertEqual(u'user1', post1.user.name)
        self.assertEqual(None, post1.other_user)

        post1.other_user = user1

        save()

        loader = StreamLoader(user1_key)
        loader.load()
        posts = loader.result
        # If the inventory doesn't load, an AttributeNotFound error would
        # arise.
        for post in posts:
            self.assertEqual(post.user.name, u'user1')
            self.assertEqual(post.other_user.name, u'user1')

        new_active_session()

        loader = AlphaLoader(u'testpost1')
        loader.load()

        posts = loader.result
        # If the inventory doesn't load, an AttributeNotFound error would
        # arise.
        for post in posts:
            self.assertEqual(post.user.name, u'user1')
            self.assertEqual(post.other_user.name, u'user1')

        new_active_session()

        post1 = Post(post1_key)
        post1.load(PostLoader)
        self.assertEqual(u'user1', post1.other_user.name)

        post1.other_user = None

        save()

        new_active_session()

        loader = StreamLoader(user1_key)
        loader.load()
        posts = loader.result
        # If the inventory doesn't load, an AttributeNotFound error would
        # arise.
        for post in posts:
            self.assertEqual(post.user.name, u'user1')
            self.assertEqual(post.other_user, None)

        new_active_session()

        loader = AlphaLoader(u'testpost1')
        loader.load()
        posts = loader.result
        # If the inventory doesn't load, an AttributeNotFound error would
        # arise.
        for post in posts:
            self.assertEqual(post.user.name, u'user1')
            self.assertEqual(post.other_user, None)

        new_active_session()

        post1 = Post(post1_key)
        post1.load(PostLoader)
        self.assertEqual(None, post1.other_user)

    def test_uninitialized_loader(self):
        """Uninitialized Loader should raise an exception"""
        self.reset_cassandra([Foo])

        # Create Persistent object.
        foo = Foo.create()
        foo_key = foo.key

        save()

        foo = Foo(foo_key)
        raised = False
        try:
            foo.load([UninitializedFooLoader])
        except UninitializedLoader:
            raised = True
        self.assertTrue(raised)

    def test_in_clause(self):
        """Loaders should combine requests using an IN clause if possible."""
        self.reset_cassandra([Small])
        # Note: I don't know how to test this other than looking at the logs
        # so watch the logs and see that there is are only two statements
        # issued to Cassandra.

        # Make 4 objects.
        small1 = Small.create()
        small1_key = small1.key
        small1.text1 = u'1text1'
        small1.text2 = u'1text2'
        small1.text3 = u'1text3'
        small1.increment('inc1', 11)
        small1.increment('inc2', 12)
        small1.increment('inc3', 13)
        small2 = Small.create()
        small2_key = small2.key
        small2.text1 = u'2text1'
        small2.text2 = u'2text2'
        small2.text3 = u'2text3'
        small2.increment('inc1', 21)
        small2.increment('inc2', 22)
        small2.increment('inc3', 23)
        small3 = Small.create()
        small3_key = small3.key
        small3.text1 = u'3text1'
        small3.text2 = u'3text2'
        small3.text3 = u'3text3'
        small3.increment('inc1', 31)
        small3.increment('inc2', 32)
        small3.increment('inc3', 33)
        small4 = Small.create()
        small4_key = small4.key
        small4.text1 = u'4text1'
        small4.text2 = u'4text2'
        small4.text3 = u'4text3'
        small4.increment('inc1', 41)
        small4.increment('inc2', 42)
        small4.increment('inc3', 43)

        save()

        # Let's load them all at once, with 3 of them having the same
        # inventory and the 4th using a different one.
        loaders = [
            # text1, text2, inc1, inc2, inc3
            SmallLoaderA(small1_key), SmallLoaderB(small1_key),
            # text1, text2, inc1
            SmallLoaderD(small2_key),
            # text1, text2, inc1, inc3
            SmallLoaderA(small3_key), SmallLoaderD(small3_key),
            # text1, text2, text3, inc1, inc2, inc3
            SmallLoaderD(small4_key), SmallLoaderC(small4_key), SmallLoaderA(small4_key)
        ]
        backend.loader.load(loaders)

        small1 = Small(small1_key)
        small2 = Small(small2_key)
        small3 = Small(small3_key)
        small4 = Small(small4_key)

        self.assertEqual(small1.text1, u'1text1')
        self.assertEqual(small1.text2, u'1text2')
        self.assertNotLoaded(small1, 'text3')
        self.assertEqual(small1.inc1, 11)
        self.assertEqual(small1.inc2, 12)
        self.assertEqual(small1.inc3, 13)
        self.assertEqual(small2.text1, u'2text1')
        self.assertEqual(small2.text2, u'2text2')
        self.assertNotLoaded(small2, 'text3')
        self.assertEqual(small2.inc1, 21)
        self.assertNotLoaded(small2, 'inc2')
        self.assertNotLoaded(small2, 'inc3')
        self.assertEqual(small3.text1, u'3text1')
        self.assertEqual(small3.text2, u'3text2')
        self.assertNotLoaded(small3, 'text3')
        self.assertEqual(small3.inc1, 31)
        self.assertNotLoaded(small3, 'inc2')
        self.assertEqual(small3.inc3, 33)
        self.assertEqual(small4.text1, u'4text1')
        self.assertEqual(small4.text2, u'4text2')
        self.assertEqual(small4.text3, u'4text3')
        self.assertEqual(small4.inc1, 41)
        self.assertEqual(small4.inc2, 42)
        self.assertEqual(small4.inc3, 43)

    def test_missing_rows(self):
        """Missing rows should not interfere with other results."""
        self.reset_cassandra([Small])

        small1 = Small.create()
        small1_key = small1.key
        small1.text1 = u'1'
        small1.increment('inc3', 1)
        small2 = Small.create()
        small2_key = small2.key
        small2.text1 = u'2'
        small2.increment('inc3', 2)
        small3 = Small.create()
        small3_key = small3.key
        small3.text1 = u'3'
        small3.increment('inc3', 3)

        save()

        batch(deletes={'b_small': [{'uuid': small2_key}],
                       'b_small__counts': [{'uuid': small2_key}]})

        small1 = Small(small1_key)
        small2 = Small(small2_key)
        small3 = Small(small3_key)

        loader1 = SmallLoaderA(small1_key)
        loader2 = SmallLoaderA(small2_key)
        loader3 = SmallLoaderA(small3_key)

        load([loader1, loader2, loader3], suppress_misses=True)

        self.assertEqual(small1.text1, u'1')
        self.assertEqual(small1.inc3, 1)
        self.assertEqual(loader2.load_failed, True)
        self.assertEqual(small3.text1, u'3')
        self.assertEqual(small3.inc3, 3)

        # We repeat the test deleting just the primary.
        new_active_session()

        small1 = Small.create()
        small1_key = small1.key
        small1.text1 = u'1'
        small1.increment('inc3', 1)
        small2 = Small.create()
        small2_key = small2.key
        small2.text1 = u'2'
        small2.increment('inc3', 2)
        small3 = Small.create()
        small3_key = small3.key
        small3.text1 = u'3'
        small3.increment('inc3', 3)

        save()

        batch(deletes={'b_small': [{'uuid': small2_key}]})

        small1 = Small(small1_key)
        small2 = Small(small2_key)
        small3 = Small(small3_key)

        loader1 = SmallLoaderA(small1_key)
        loader2 = SmallLoaderA(small2_key)
        loader3 = SmallLoaderA(small3_key)

        load([loader1, loader2, loader3], suppress_misses=True)

        self.assertEqual(small1.text1, u'1')
        self.assertEqual(small1.inc3, 1)
        self.assertEqual(loader2.load_failed, True)
        self.assertEqual(small3.text1, u'3')
        self.assertEqual(small3.inc3, 3)

        # We repeat the test deleting just the count.
        new_active_session()

        small1 = Small.create()
        small1_key = small1.key
        small1.text1 = u'1'
        small1.increment('inc3', 1)
        small2 = Small.create()
        small2_key = small2.key
        small2.text1 = u'2'
        small2.increment('inc3', 2)
        small3 = Small.create()
        small3_key = small3.key
        small3.text1 = u'3'
        small3.increment('inc3', 3)

        save()

        batch(deletes={'b_small__counts': [{'uuid': small2_key}]})

        small1 = Small(small1_key)
        small2 = Small(small2_key)
        small3 = Small(small3_key)

        loader1 = SmallLoaderA(small1_key)
        loader2 = SmallLoaderA(small2_key)
        loader3 = SmallLoaderA(small3_key)

        load([loader1, loader2, loader3], suppress_misses=True)

        self.assertEqual(small1.text1, u'1')
        self.assertEqual(small1.inc3, 1)
        self.assertEqual(small2.text1, u'2')
        # xxx put this back after we migration all incr nones to 0 and
        # remove the incrnonezero hack.
        self.assertEqual(small2.inc3, 0)#None)
        self.assertEqual(small3.text1, u'3')
        self.assertEqual(small3.inc3, 3)

    def test_loader_redundancy(self):
        """Loaded loaders should not load again in the same Session."""
        self.reset_cassandra([Foo, User, Post])

        # We will load a loader and then delete the record from Cassandra
        # and load again (in the same session.)  If our redundancy system
        # is working we will not notice that the record has been deleted.

        # First test if our test mechanism works.
        foo = Foo.create()
        foo_key = foo.key

        save()

        # This should raise no error.
        foo = Foo(foo_key)
        raised = False
        try:
            foo.load([FooLoader])
        except:
            raised = True
        self.assertFalse(raised)

        new_active_session()

        # We delete the data from Cassandra so that a load will raise an error.
        batch(deletes={'b_foo': [{'uuid': foo_key}]})

        foo = Foo(foo_key)
        raised = False
        try:
            foo.load([FooLoader])
        except NullEnvironmentError:
            raised = True
        self.assertTrue(raised)

        # Now we know a deleted row raises an error, let's create a new foo.
        foo = Foo.create()
        foo_key = foo.key
        foo.uni_field = u'abcd123'

        # Note: This test file's 'save' function calls new_active_session.
        save()

        # Let's load the Foo into the session.
        foo = Foo(foo_key)
        raised = False
        try:
            foo.load([FooLoader])
        except:
            raised = True
        self.assertFalse(raised)

        # Now we delete the row.
        batch(deletes={'b_foo': [{'uuid': foo_key}]})

        # And load again.
        raised = False
        try:
            foo.load([FooLoader])
        except:
            raised = True
        self.assertFalse(raised)

        # And to be sure, was the data loaded?
        self.assertEqual(foo.uni_field, u'abcd123')

        # Let's do this again this time with an index loader.

        user1 = User.create()
        user1_key = user1.key
        user1.name = u'user1'

        post1 = Post.create()
        post1_key = post1.key
        post1.posted_on = datetime(2003, 5, 17, 22, 58, 59)
        post1.text = u'testpost1'
        post1.user = user1
        post1.kind = PostEnum[u'unflagged']

        post2 = Post.create()
        post2_key = post2.key
        post2.posted_on = datetime(2003, 5, 18, 22, 58, 59)
        post2.text = u'testpost2'
        post2.user = user1
        post2.kind = PostEnum[u'unflagged']

        save()

        loader = StreamLoader(user1_key)
        loader.load()
        posts = loader.result
        self.assertEqual(2, len(posts))

        batch(deletes={
            'b_post_stream': [{
                'user': user1_key,
                'posted_on': to_hsntime(datetime(2003, 5, 18, 22, 58, 59))}
            ]})

        loader = StreamLoader(user1_key)
        loader.load()
        posts = loader.result
        self.assertEqual(2, len(posts))

        new_active_session()

        loader = StreamLoader(user1_key)
        loader.load()
        posts = loader.result
        self.assertEqual(1, len(posts))

        # Now let's test this with kwargs.

        user1 = User.create()
        user1_key = user1.key
        user1.name = u'user1'

        post1 = Post.create()
        post1_key = post1.key
        post1.posted_on = datetime(2003, 5, 17, 22, 58, 59)
        post1.text = u'testpost1'
        post1.user = user1
        post1.kind = PostEnum[u'unflagged']

        post2 = Post.create()
        post2_key = post2.key
        post2.posted_on = datetime(2003, 5, 18, 22, 58, 59)
        post2.text = u'testpost2'
        post2.user = user1
        post2.kind = PostEnum[u'unflagged']

        post3 = Post.create()
        post3_key = post3.key
        post3.posted_on = datetime(2003, 5, 19, 22, 58, 59)
        post3.text = u'testpost3'
        post3.user = user1
        post3.kind = PostEnum[u'unflagged']

        save()

        loader = StreamLoader(user1_key,
                              start=datetime(2003, 5, 18, 22, 58, 59),
                              count=1)
        loader.load()
        posts = loader.result
        self.assertEqual(1, len(posts))
        self.assertEqual(posts[0].text, u'testpost2')

        batch(deletes={
            'b_post_stream': [{
                'user': user1_key,
                'posted_on': to_hsntime(datetime(2003, 5, 18, 22, 58, 59))}
            ]})

        loader = StreamLoader(user1_key,
                              start=datetime(2003, 5, 18, 22, 58, 59),
                              count=1)
        loader.load()
        posts = loader.result
        self.assertEqual(1, len(posts))
        self.assertEqual(posts[0].text, 'testpost2')

        new_active_session()

        loader = StreamLoader(user1_key,
                              start=datetime(2003, 5, 18, 22, 58, 59),
                              count=1)
        loader.load()
        posts = loader.result
        self.assertEqual(1, len(posts))
        self.assertEqual(posts[0].text, u'testpost1')


    def test_other_assignment_errors(self):
        """Assignment should not raise errors under a certain condition."""
        # This is a test in response to a bug identified by Mike.
        self.reset_cassandra([Post, User])

        # Create an object with non-empty values for all index keys.
        post = Post.create()
        post_key = post.key
        user = User.create()
        user_key = user.key
        post.user = user
        post.posted_on = datetime(2003, 5, 17, 22, 58, 59)
        post_text_value = u'\xe6\u2264\u2265'
        post.text = post_text_value

        save()

        # Assert the index table was written.
        loader = StreamLoader(user_key)
        loader.load()
        posts = loader.result
        self.assertEqual(1, len(posts))

        new_active_session()

        # Set a non-key value that will write to the index table.
        post = Post(post_key)
        post.load(PostKeyLoader)
        other_user = User.create()
        other_user_key = other_user.key
        post.other_user = other_user

        save()

        # Load from the index table, confirm value was set.
        loader = StreamLoader(user_key)
        loader.load()
        posts = loader.result
        self.assertEqual(1, len(posts))
        self.assertEqual(other_user_key, posts[0].other_user.key)

    def test_empty_indexes_on_create_and_update(self):
        """Empty keys (that are in composite keys) don't write to indexes."""
        self.reset_cassandra([Post, User])

        # Create a Post without enough information to save to the index
        # tables.
        post1 = Post.create()
        post1_key = post1.key

        # Test a 'later' save.
        backend.Cassandra().save_later()

        save()

        # Note: We can't confirm the absense of this post in the index tables
        # because the index tables are not looked up by the one piece of
        # information we have, the post's uuid.  (they are looked up by
        # user or text, for example.)

        # Create another Post without enough information to save to the index
        # tables.
        post2 = Post.create()
        post2_key = post2.key

        # Test a 'now' save.
        backend.Cassandra().save()

        save()

        # Make a User so that we can attach it to a Post and write to an
        # index table.
        # (note here we will be testing the case of a key object being
        # created in the same session.  Below we do a test
        # where the key object was created and saved previously.)

        user1 = User.create()
        user1_key = user1.key

        post1 = Post(post1_key)
        post2 = Post(post2_key)

        load([PostLoader(post1_key), PostLoader(post2_key)])

        # Set sufficient information for post1 to write to the 'stream' and
        # 'withprimary' index tables.
        post1.user = user1
        post1.posted_on = datetime(2003, 5, 17, 22, 58, 59)
        # Post does does not have sufficient information to write to any index.
        post2_text_value = u'\xf8\xb4\xa8'
        post2.text = post2_text_value

        save()

        # Confirm post1 is the only Post in the Post.stream index keyed on
        # user1.
        loader = StreamLoader(user1_key)
        loader.load()
        posts = loader.result
        self.assertEqual(1, len(posts))
        self.assertEqual(posts[0].key, post1_key)
        post1 = Post(post1_key)
        self.assertEqual(post1.text, None)

        loader = AlphaLoader(post2_text_value)
        loader.load()
        posts = loader.result
        self.assertEqual(1, len(posts))

        # Confirm post2's new text (which is a composite key attribute) is
        # found in the primary table.
        post2 = Post(post2_key)
        post2.load(PostLoader)
        self.assertEqual(post2_text_value, post2.text)

        # Here we make Post2's user in it's own session, thus testing update.
        new_active_session()

        user2 = User.create()
        user2_key = user2.key

        save()

        # Let's test some error detection for a moment.
        # Here we test the rule where if you are going to set a key's value
        # you first need to know (load or promote) the key's existing value.
        user2 = User(user2_key)
        post2 = Post(post2_key)

        raised = False
        try:
            post2.user = user2
        except KeyNotLoaded:
            raised = True
        self.assertTrue(raised)

        new_active_session()

        # Again, this time with posted_on instead of user.
        post2 = Post(post2_key)

        raised = False
        try:
            post2.posted_on = datetime(2003, 5, 17, 22, 58, 59)
        except KeyNotLoaded:
            raised = True
        self.assertTrue(raised)

        # Even if the value we are setting is known, at least one other key
        # must be known (or we get KeyNotLoaded error) to be empty (or else we
        # could raise an AllAttributesNotLoaded error)
        # We test that here, by promoting a None into those attributes.

        # If user is known to be None, we still need to know if the other key
        # (posted_on) is known.

        new_active_session()

        user2 = User(user2_key)
        post2 = Post(post2_key)
        # We must promote or load the existing value for post2.user before
        # we update it.  Here we promote it.
        post2.environment.promote(post2, 'user', None)

        # We raise KeyNotLoaded error because we do not know information to
        # determine if we write to an index table.
        raised = False
        try:
            post2.user = user2
        except KeyNotLoaded:
            raised = True
        self.assertTrue(raised)

        new_active_session()

        user2 = User(user2_key)
        post2 = Post(post2_key)
        # This loads the existing value for all keys.
        post2.load(PostKeyLoader)

        # We raise AllAttributesNotLoaded error because we do not have the
        # information we need to write to the 'withprimary' index, which
        # requires the uuid (known by default in the backend) and now the
        # user.  It would have to write all attributes to this table, but
        # all attributes have not been loaded, just the key attributes were
        # loaded with the PostKeyLoader.
        raised = False
        try:
            post2.user = user2
        except AllAttributesNotLoaded:
            raised = True
        self.assertTrue(raised)

        # Let's repeat the previous, but instead of using the PostKeyLoader
        # (which only loads the keys) let's use the PostLoader, which
        # loads everything.
        new_active_session()

        user2 = User(user2_key)
        post2 = Post(post2_key)
        # This loads the existing value for all attributes.
        post2.load(PostLoader)

        raised = False
        try:
            post2.user = user2
        except AllAttributesNotLoaded:
            raised = True
        self.assertFalse(raised)

        save()

    def test_loader_write_onlies(self):
        """Loaders should not fail if write onlies are present."""
        self.reset_cassandra([Foo, Baz])

        foo = Foo.create()
        foo_key = foo.key
        baz = Baz.create()
        baz_key = baz.key
        baz.int_field = 42  # this is in FooLoader inventory.
        foo.baz_many = baz

        save()

        foo = Foo(foo_key)
        # This will make a write-only
        self.assertTrue(foo.baz_many.write_only)
        foo.baz_many.int_field = 100

        # This should not raise an error.
        foo.load(FooLoader)
        self.assertFalse(foo.baz_many.write_only)
        self.assertEqual(foo.baz_many.int_field, 100)

        foo = Foo.create()
        foo_key = foo.key
        baz = Baz.create()
        baz_key = baz.key
        baz.int_field = 42  # this is in FooLoader inventory.
        foo.baz_many = baz

        save()

        foo = Foo(foo_key)
        # This will make a write-only
        self.assertTrue(foo.baz_many.write_only)

        # This should not raise an error.
        foo.load(FooLoader)
        self.assertFalse(foo.baz_many.write_only)
        self.assertEqual(foo.baz_many.int_field, 42)
