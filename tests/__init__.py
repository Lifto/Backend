
import logging
from unittest import TestCase as UnitTestCase

import elixir
import sqlalchemy.orm

from hsn.tests.support.testcases import _log_doc

import util.database
from util.cache import flush
from util.cassandra import create_cassandra_keyspace, \
                           drop_cassandra_keyspace
from backend import new_active_session
from backend.core import cassandra_create_column_families
from backend.core import setup_backend_mapping
from backend.core import setup_cache_inventory, setup_loader_inventory

logger = logging.getLogger('hsn.backend.tests')

OLD_DB_NAME = None
def _setup_module(db_name, collection, metadata, session, cache_list, loader_list):
    # Turns out we need to monkey-patch util.database or some internal
    # functions won't work.
    global OLD_DB_NAME
    OLD_DB_NAME = util.database.db_name
    util.database.db_name = db_name
    util.database._create_database(db_name)
    
    elixir.metadata = metadata
    elixir.session = session
    
    elixir.entity.setup_entities(collection)
    metadata.create_all()
    
    setup_backend_mapping(collection)

    setup_cache_inventory(cache_list)
    setup_loader_inventory(loader_list)

def _teardown_module(db_name, collection, metadata, session, engine):
    session.close()

    elixir.entity.cleanup_entities(collection)
    sqlalchemy.orm.clear_mappers()
    collection.clear()
    metadata.drop_all()
    metadata.clear()

    session.rollback()
    session.remove()

    engine.pool.dispose()

    util.database._drop_database(db_name)
    util.database.db_name = OLD_DB_NAME

    elixir.metadatas.clear()

class TestCase(UnitTestCase):
    collection = []  # Set this so reset_cassandra will work.

    @classmethod
    def reset_cassandra(cls, using_entities=None):
        """Drop and recreate Cassandra keyspace, set up the given entity classes.

        Call this at the start of your test.

        Note if this is failing for you make sure you assigned a value to
        the TestCase's class variable 'collection', or that you provided
        a user_entities argument.

        using_entities -- List of entity_classes this test uses.
                          None means use TestCase.collection

        """
        try:
            drop_cassandra_keyspace(retries=0)
        except:
            pass
        create_cassandra_keyspace(retries=0)
        if using_entities is None:
            using_entities = [p.entity_class for p in cls.collection]
        for entity_class in using_entities:
            cassandra_create_column_families(entity_class)

    def setUp(self):
        _log_doc(self, self.shortDescription(), logger)
        flush()
        new_active_session()
        super(TestCase, self).setUp()
        