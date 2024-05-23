
import elixir
import elixir.collection
import elixir.entity
from sqlalchemy import MetaData, create_engine
import sqlalchemy.orm

import util.database
from util.cassandra import Fetcher
from util.cassandra import connection as cassandra_connection
from util.log import logger
import backend
from backend import new_active_session
from backend.environments.cache import Cache
from backend.environments.core import Entity
from backend.loader import InventoryLoader
from backend.schema import IncrementField, \
                           IntegerField, \
                           ManyToNPOneUUIDField, \
                           OneToNPManyField, \
                           Persistent, \
                           UnicodeField, \
                           UUIDField
from backend.tests import TestCase, _setup_module, _teardown_module

db_name = 'test_hybrid'

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
    cache_list = [MtOCache, OtMCache]
    loader_list = [MtOLoader, MtOCLoader, OtMCLoader]

    _setup_module(db_name, collection, metadata, session, cache_list, loader_list)

def teardown_module():
    _teardown_module(db_name, collection, metadata, session, engine)

#-- Many to One ---------------------------------------------------------------

class MtOPersist(Persistent):
    elixir.using_options(tablename='mto_object', collection=collection)
    uuid = UUIDField(primary_key=True)
    cassandra_many_to_one = ManyToNPOneUUIDField('MtOCPersist',
                                                 inverse='both_one_to_many')
    uni_field = UnicodeField()
    int_field = IntegerField(default=0)
    inc_field = IncrementField(default=0)

class MtO(Entity):
    pass
MtO.persistent_class = MtOPersist
MtO.entity_class = MtO
MtOPersist.entity_class = MtO
MtOPersist.persistent_class = MtOPersist

class MtOLoader(InventoryLoader):
    entity_class = MtO
    inventory = [
        'uuid',
        'cassandra_many_to_one',
        'uni_field',
        'int_field',
        'inc_field'
    ]

class MtOCPersist(Persistent):
    saves_to_postgresql = False
    elixir.using_options(tablename='mtoc_object', collection=collection)
    uuid = UUIDField(primary_key=True)
    cassandra_one_to_many = OneToNPManyField('MtOCPersist',
                                             inverse='cassandra_many_to_one')
    cassandra_many_to_one = ManyToNPOneUUIDField('MtOCPersist',
                                                 inverse='cassandra_one_to_many')
    both_one_to_many = OneToNPManyField('MtOPersist', inverse='cassandra_many_to_one')
    postgresql_one_to_many = OneToNPManyField('MtOPPersist', inverse='cassandra_many_to_one')
    uni_field = UnicodeField()
    int_field = IntegerField(default=0)
    inc_field = IncrementField(default=0)

class MtOC(Entity):
    pass
MtOC.persistent_class = MtOCPersist
MtOC.entity_class = MtOC
MtOCPersist.entity_class = MtOC
MtOCPersist.persistent_class = MtOCPersist

class MtOCLoader(InventoryLoader):
    entity_class = MtOC
    inventory = [
        'uuid',
        'cassandra_many_to_one',
        'uni_field',
        'int_field',
        'inc_field'
    ]


class MtOPPersist(Persistent):
    saves_to_cassandra = False
    elixir.using_options(tablename='mtop_object', collection=collection)
    uuid = UUIDField(primary_key=True)
    cassandra_many_to_one = ManyToNPOneUUIDField('MtOCPersist',
                                                 inverse='postgresql_one_to_many')
    uni_field = UnicodeField()
    int_field = IntegerField(default=0)
    inc_field = IncrementField(default=0)

class MtOP(Entity):
    pass
MtOP.persistent_class = MtOPPersist
MtOP.entity_class = MtOP
MtOPPersist.entity_class = MtOP
MtOPPersist.persistent_class = MtOPPersist

class MtOCache(Cache):
    entity_class = MtOP
    inventory = [
        'uuid',
        'cassandra_many_to_one',
        'uni_field',
        'int_field',
        'inc_field'
    ]


#-- One to Many ---------------------------------------------------------------

class OtMPersist(Persistent):
    elixir.using_options(tablename='otm_object', collection=collection)
    uuid = UUIDField(primary_key=True)
    cassandra_one_to_many = OneToNPManyField('OtMCPersist',
                                             inverse='both_many_to_one')
    uni_field = UnicodeField()
    int_field = IntegerField(default=0)
    inc_field = IncrementField(default=0)

class OtM(Entity):
    pass
OtM.persistent_class = OtMPersist
OtM.entity_class = OtM
OtMPersist.entity_class = OtM
OtMPersist.persistent_class = OtMPersist

class OtMCPersist(Persistent):
    saves_to_postgresql = False
    elixir.using_options(tablename='otmc_object', collection=collection)
    uuid = UUIDField(primary_key=True)
    cassandra_many_to_one = ManyToNPOneUUIDField('OtMCPersist',
                                                 inverse='cassandra_one_to_many')
    cassandra_one_to_many = OneToNPManyField('OtMCPersist',
                                             inverse='cassandra_many_to_one')
    both_many_to_one = ManyToNPOneUUIDField('OtMPersist', inverse='cassandra_one_to_many')
    postgresql_many_to_one = ManyToNPOneUUIDField('OtMPPersist', inverse='cassandra_one_to_many')
    uni_field = UnicodeField()
    int_field = IntegerField(default=0)
    inc_field = IncrementField(default=0)

class OtMC(Entity):
    pass
OtMC.persistent_class = OtMCPersist
OtMC.entity_class = OtMC
OtMCPersist.entity_class = OtMC
OtMCPersist.persistent_class = OtMCPersist

class OtMCLoader(InventoryLoader):
    entity_class = OtMC
    inventory = [
        'uuid',
        'both_many_to_one',
        'cassandra_many_to_one',
        'postgresql_many_to_one',
        'uni_field',
        'int_field',
        'inc_field'
    ]


class OtMPPersist(Persistent):
    saves_to_cassandra = False
    elixir.using_options(tablename='otmp_object', collection=collection)
    uuid = UUIDField(primary_key=True)
    cassandra_one_to_many = OneToNPManyField('OtMCPersist',
                                             inverse='postgresql_many_to_one')
    uni_field = UnicodeField()
    int_field = IntegerField(default=0)
    inc_field = IncrementField(default=0)

class OtMP(Entity):
    pass
OtMP.persistent_class = OtMPPersist
OtMP.entity_class = OtMP
OtMPPersist.entity_class = OtMP
OtMPPersist.persistent_class = OtMPPersist

class OtMCache(Cache):
    entity_class = OtMP
    inventory = [
        'uuid',
        'cassandra_one_to_many',
        'uni_field',
        'int_field',
        'inc_field'
    ]

def get_cassandra(entity, attribute_name):
    column_family = entity.entity_class.get_primary_column_family()
    return get_cassandra_raw(column_family, attribute_name, entity.entity_class.key_attribute_name, entity.key)

def get_cassandra_raw(column_family, attribute_name, key_attribute_name, key):
    fetcher = Fetcher(cf_name=column_family, fields=(attribute_name,))
    fetcher.add_column_value_relation(key_attribute_name, key)
    result = fetcher.fetch_first()
    #logger.error(result)
    if result:
        return result[0]
    return None

def get_cassandra_all(column_family, attribute_name):
    stmt = "SELECT %s FROM %s LIMIT 100" % (
        attribute_name, column_family)
    #logger.error(stmt)
    with cassandra_connection as connection:
        cursor = connection.cursor()
        cursor.execute(str(stmt))
        result = cursor.fetchmany(10000)
        return [r[0] for r in result]

def table_exists(table_name):
    result = None
    statement = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='%s'"
    statement = statement % table_name
    #from backend.core import get_session
    import backend.core
    session = backend.core.get_session()
    result = session.execute(statement)
    result = list(result)
    logger.error(result)
    if result:
        return True
    else:
        return False

def save():
    backend.save(try_later_on_this_thread_first=True)
    new_active_session()

class assertIncomplete(object):
    def assertOneInIncompleteSet(self, entity, set_handle):
        self.assertEqual(
            'IncompleteSet',
            type(set_handle._get_value()).__name__)
        self.assertEqual(1, len(set_handle._get_value()._contains))
        self.assertTrue(entity in set_handle)

class TestHybrid(TestCase, assertIncomplete):
    collection = collection

    # Note this must be disabled, it doesn't seem to work unless we only run
    # this test file.
#    def test_tables(self):
#        """Test table creation (and non creation of non sql tables)."""
#        self.reset_cassandra()
#
#        # Verify the model in postgresql.
#        for persistent_class in collection:
#            logger.error(persistent_class)
#            if persistent_class.saves_to_postgresql:
#                self.assertTrue(table_exists(persistent_class.table))
#            else:
#                self.assertFalse(table_exists(persistent_class.table))

    def test_many_to_one(self):
        "Many to One."
        self.reset_cassandra()

        # Create Persistent object.
        both = MtO.create()
        both_key = both.key
        self.assertEqual(None, both.cassandra_many_to_one)
        self.assertEqual(None, both.uni_field)
        self.assertEqual(0, both.int_field)
        self.assertEqual(0, both.inc_field)

        save()

        both = MtO(both_key)
        both.load(MtOLoader)
        self.assertEqual(None, both.cassandra_many_to_one)
        self.assertEqual(None, both.uni_field)
        self.assertEqual(0, both.int_field)
        self.assertEqual(0, both.inc_field)

        # Create Cassandra and Postrgesql only objects.
        cassandra = MtOC.create()
        cassandra_key = cassandra.key
        other_cassandra = MtOC.create()
        other_cassandra_key = other_cassandra.key
        postgresql = MtOP.create()
        postgresql_key = postgresql.key

        both.cassandra_many_to_one = cassandra
        postgresql.cassandra_many_to_one = cassandra

        save()

        both = MtO(both_key)
        cassandra = MtOC(cassandra_key)
        other_cassandra = MtOC(other_cassandra_key)
        postgresql = MtOP(postgresql_key)

        cassandra.load(MtOCLoader)
        self.assertEqual(None, cassandra.cassandra_many_to_one)
        self.assertEqual(None, cassandra.uni_field)
        self.assertEqual(0, cassandra.int_field)
        self.assertEqual(0, cassandra.inc_field)

        both.load(MtOLoader)
        self.assertEqual(cassandra, both.cassandra_many_to_one)
        self.assertOneInIncompleteSet(both, cassandra.both_one_to_many)

        postgresql.load(MtOCache)
        self.assertEqual(cassandra, postgresql.cassandra_many_to_one)
        self.assertOneInIncompleteSet(postgresql,
                                      cassandra.postgresql_one_to_many)

        both.cassandra_many_to_one = other_cassandra
        postgresql.cassandra_many_to_one = other_cassandra
        cassandra.cassandra_many_to_one = other_cassandra

        postgresql.save(MtOCache)
        save()

        both = MtO(both_key)
        cassandra = MtOC(cassandra_key)
        other_cassandra = MtOC(other_cassandra_key)
        postgresql = MtOP(postgresql_key)

        both.load(MtOLoader)
        self.assertEqual(other_cassandra, both.cassandra_many_to_one)
        self.assertOneInIncompleteSet(both, other_cassandra.both_one_to_many)

        postgresql.load(MtOCache)
        self.assertEqual(other_cassandra, postgresql.cassandra_many_to_one)
        self.assertOneInIncompleteSet(postgresql,
                                      other_cassandra.postgresql_one_to_many)

        cassandra.load(MtOCLoader)
        self.assertEqual(other_cassandra, cassandra.cassandra_many_to_one)
        self.assertOneInIncompleteSet(cassandra, other_cassandra.cassandra_one_to_many)

        both.cassandra_many_to_one = None
        postgresql.cassandra_many_to_one = None
        cassandra.cassandra_many_to_one = None

        postgresql.save(MtOCache)
        save()

        both = MtO(both_key)
        cassandra = MtOC(cassandra_key)
        other_cassandra = MtOC(other_cassandra_key)
        postgresql = MtOP(postgresql_key)

        both.load(MtOLoader)
        self.assertEqual(None, both.cassandra_many_to_one)

        postgresql.load(MtOCache)
        self.assertEqual(None, postgresql.cassandra_many_to_one)

        cassandra.load(MtOCLoader)
        self.assertEqual(None, cassandra.cassandra_many_to_one)

        other_cassandra.load(MtOCLoader)
        self.assertEqual(None, other_cassandra.cassandra_many_to_one)

        cassandra.both_one_to_many.add(both)
        cassandra.cassandra_one_to_many.add(other_cassandra)
        cassandra.postgresql_one_to_many.add(postgresql)

        postgresql.save(MtOCache)
        save()

        both = MtO(both_key)
        cassandra = MtOC(cassandra_key)
        other_cassandra = MtOC(other_cassandra_key)
        postgresql = MtOP(postgresql_key)

        both.load(MtOLoader)
        self.assertEqual(cassandra, both.cassandra_many_to_one)
        self.assertOneInIncompleteSet(both, cassandra.both_one_to_many)

        postgresql.load(MtOCache)
        self.assertEqual(cassandra, postgresql.cassandra_many_to_one)
        self.assertOneInIncompleteSet(postgresql,
                                      cassandra.postgresql_one_to_many)

        other_cassandra.load(MtOCLoader)
        self.assertEqual(cassandra, other_cassandra.cassandra_many_to_one)
        self.assertOneInIncompleteSet(other_cassandra, cassandra.cassandra_one_to_many)

        return

    def test_one_to_many(self):
        "One to Many."
        self.reset_cassandra()

        both = OtM.create()
        both_key = both.key
        cassandra = OtMC.create()
        cassandra_key = cassandra.key
        other_cassandra = OtMC.create()
        other_cassandra_key = other_cassandra.key
        postgresql = OtMP.create()
        postgresql_key = postgresql.key

        save()

        both = OtM(both_key)
        cassandra = OtMC(cassandra_key)
        other_cassandra = OtMC(other_cassandra_key)
        postgresql = OtMP(postgresql_key)

        cassandra.load(OtMCLoader)

        self.assertEqual(None, cassandra.both_many_to_one)
        self.assertEqual(None, cassandra.cassandra_many_to_one)
        self.assertEqual(None, cassandra.postgresql_many_to_one)

        cassandra.both_many_to_one = both
        cassandra.cassandra_many_to_one = other_cassandra
        cassandra.postgresql_many_to_one = postgresql

        save()

        both = OtM(both_key)
        cassandra = OtMC(cassandra_key)
        other_cassandra = OtMC(other_cassandra_key)
        postgresql = OtMP(postgresql_key)

        cassandra.load(OtMCLoader)

        self.assertEqual(both, cassandra.both_many_to_one)
        self.assertEqual(other_cassandra, cassandra.cassandra_many_to_one)
        self.assertEqual(postgresql, cassandra.postgresql_many_to_one)
        self.assertOneInIncompleteSet(cassandra, both.cassandra_one_to_many)
        self.assertOneInIncompleteSet(cassandra, other_cassandra.cassandra_one_to_many)
        self.assertOneInIncompleteSet(cassandra, postgresql.cassandra_one_to_many)

        cassandra.both_many_to_one = None
        cassandra.cassandra_many_to_one = None
        cassandra.postgresql_many_to_one = None

        save()

        both = OtM(both_key)
        cassandra = OtMC(cassandra_key)
        other_cassandra = OtMC(other_cassandra_key)
        postgresql = OtMP(postgresql_key)

        cassandra.load(OtMCLoader)

        self.assertEqual(None, cassandra.both_many_to_one)
        self.assertEqual(None, cassandra.cassandra_many_to_one)
        self.assertEqual(None, cassandra.postgresql_many_to_one)
        both.cassandra_one_to_many.add(cassandra)
        other_cassandra.cassandra_one_to_many.add(cassandra)
        postgresql.cassandra_one_to_many.add(cassandra)

        save()

        both = OtM(both_key)
        cassandra = OtMC(cassandra_key)
        other_cassandra = OtMC(other_cassandra_key)
        postgresql = OtMP(postgresql_key)

        cassandra.load(OtMCLoader)

        self.assertEqual(both, cassandra.both_many_to_one)
        self.assertEqual(other_cassandra, cassandra.cassandra_many_to_one)
        self.assertEqual(postgresql, cassandra.postgresql_many_to_one)
        self.assertOneInIncompleteSet(cassandra, both.cassandra_one_to_many)
        self.assertOneInIncompleteSet(cassandra, other_cassandra.cassandra_one_to_many)
        self.assertOneInIncompleteSet(cassandra, postgresql.cassandra_one_to_many)

        both.cassandra_one_to_many.discard(cassandra)
        other_cassandra.cassandra_one_to_many.discard(cassandra)
        postgresql.cassandra_one_to_many.discard(cassandra)

        save()

        both = OtM(both_key)
        cassandra = OtMC(cassandra_key)
        other_cassandra = OtMC(other_cassandra_key)
        postgresql = OtMP(postgresql_key)

        cassandra.load(OtMCLoader)

        self.assertEqual(None, cassandra.both_many_to_one)
        self.assertEqual(None, cassandra.cassandra_many_to_one)
        self.assertEqual(None, cassandra.postgresql_many_to_one)

        return
