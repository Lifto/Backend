from datetime import date, datetime, time, timedelta
from itertools import izip
from logging import getLogger, INFO
from struct import pack, unpack
import threading
from time import sleep
from uuid import uuid4, UUID

import sqlalchemy
from cql import ProgrammingError
from decorator import decorator
from django.conf import settings
import elixir
from psycopg2 import OperationalError

from backend.exceptions import IllegalSchema
import util.cassandra
from util.cassandra import batch, execute, query, Fetcher, ColumnValueRelation
from util.database import cursor
from util.helpers import pluralize, ProgressLogger
from util.when import from_hsntime, from_hsndate, from_timestorage


logger = getLogger("hsn.backend")


# This global holds a mapping of Entity class name (ex: 'Obj', 'FeedEntry') to
# its EntityClass.
_name_to_entity_class = {}

# For now we just hard-code this.
# The idea is that backend operates on a model. At the moment the model
# is required to live in './model'. If it does not live there, this value
# must be different.
_repository = './model'

# Decorator
def retry_operational_error():
    """Decorator for retrying when an OperationalError is raised.

    An OperationError can be raised due to (among other reasons) a connection
    to SQL that has been restarted.  In such a case a retry is sufficient to
    reestablish the connection.  If the retry fails the exception is allowed
    to propagate.

    If the decorated function is called with a dirty session (or one holding
    pending object deletes) no retry will be attempted.

    """
    def retry_decorator(target):
        def wrapper(target, *args, **kwargs):
            # Don't retry if the session is dirty or entites have been deleted.
            if get_session().dirty or get_session().deleted:
                return target(*args, **kwargs)

            try:
                return target(*args, **kwargs)
            except (OperationalError, SystemError), e:
                logger.info("retrying %s due to %s(%s)", target.func_name, type(e), e)
                # The session must be removed or else you get complaints that
                # you are trying to use a closed connection.
                remove_session()

                # Run target again.
                return target(*args, **kwargs)

        return decorator(wrapper)(target)
    return retry_decorator

# This is a hack to get around circular imports.
def get_class(kls):
    """Get the class object for the given string.

    for example: Obj is returned for 'model.obj.Obj'

    """
    if type(kls) == type:
        return kls
    parts = kls.split('.')
    module = ".".join(parts[:-1])
    m = __import__(module)
    for comp in parts[1:]:
        m = getattr(m, comp)
    return m

def generate_id():
    """Generate a unique UUID which may be used as a UUID key in Backend."""
    return uuid4()

def _class_repr(o):
    """Print 'Foo' for 'bar.baz.Foo'."""
    return str(o.__class__).split('.')[-1:][0].split("'")[0]

def _field_repr(field):
    """Makes a __repr__ derived from the class name and field name."""
    return "%s(%s)" % (_class_repr(field), field.name)

# -- The Backend Session ------------------------------------------------------

ACTIVE_SESSION = threading.local()

def get_active_session_or_none():
    """Gets the active session, but does not create one if there isn't one."""
    try:
        return ACTIVE_SESSION.value
    except AttributeError:
        return None
    
def get_active_session():
    """Gets the active session, and makes a new one if it was None."""
    try:
        session = ACTIVE_SESSION.value
    except AttributeError:
        session = None
    if not session:
        logger.debug("-------- get_active_session making session -----------")
        from backend.environments.session import Session
        session = Session()
        ACTIVE_SESSION.value = session
    return session

def save(*args, **kwargs):
    """Shortcut to call save on the active session."""
    get_active_session().save(*args, **kwargs)
    close_active_session()

def load(*args, **kwargs):
    """Shortcut to call load on the active session."""
    return get_active_session().load(*args, **kwargs)

def new_active_session():
    """Closes the old session then makes and returns a new session."""
    logger.debug("--------- new active session ---------")
    # Close the active session, if there is one.
    close_active_session()

    # Create a new session and set the thread local global.
    from backend.environments.session import Session
    new_session = Session()
    ACTIVE_SESSION.value = new_session
    # Return the new active session.
    return new_session

def close_active_session():
    """Removes references to backend resources that are kept in globals."""
    logger.debug("--------- close active session ----------")
    try:
        current_session = ACTIVE_SESSION.value
        if current_session:
            current_session.session_closed = True
    except AttributeError:
        pass
    ACTIVE_SESSION.value = None
    from backend.environments.persistent import (MainPersistent,
        AutocompleteDetector, ExistsPersistent,
        SearchDetector)
    MainPersistent.close()
    AutocompleteDetector.close()
    ExistsPersistent.close()
    SearchDetector.close()

def cassandra_create_migration_column_family(connection=None, cursor=None):
    logger.info('creating Cassandra migration version column family')
    statement = 'CREATE COLUMNFAMILY b__migration_version (key text PRIMARY KEY, major int, minor int, micro int)'
    execute(statement, consistency_level='QUORUM', connection=connection, cursor=cursor)

def cassandra_drop_migration_column_family():
    logger.info('dropping Cassandra migration version column family')
    statement = 'DROP COLUMNFAMILY b__migration_version'
    execute(statement, consistency_level='QUORUM')

def cassandra_create_column_families(entity_class,
                                     connection=None, cursor=None):
    _cassandra_create_column_families(entity_class, None,
                                      connection=connection, cursor=cursor)
    for index in entity_class.persistent_class.indexes.keys():
        _cassandra_create_column_families(entity_class, index,
                                          connection=connection, cursor=cursor)

@query
def _cassandra_create_column_families(entity_class, index=None,
                                      connection=None, cursor=None):
    """Add an index's column families to Cassandra for a given Entity class.

    entity_class -- Class for which column families will be made.
    index        -- Name of entity's composite_key to make column families for.

    """
    if not entity_class.persistent_class.saves_to_cassandra:
        return

    if index is None:
        scalar_table_name = entity_class.get_primary_column_family()
        # Our CQL gets mussed if we try to use a column name 'token'
        _kan = entity_class.key_attribute_name
        if _kan == 'token':
            _kan = 'hsn_token'
        index_attrs = (_kan,)
    else:
        scalar_table_name = entity_class.get_index_column_family(index)
        index_attrs = entity_class.persistent_class.indexes[index]

    # Note: See the migration note at the bottom of this file.
    from backend.schema import ManyToMany, Incrementer
    counter_attributes = []
    regular_attributes = []
    for a in entity_class.attributes:
        attribute = entity_class.get_attribute(a)
        if isinstance(attribute, ManyToMany):
            raise NotImplementedError('%s not supported' % attribute)
        elif isinstance(attribute, Incrementer):
            counter_attributes.append(attribute)
        else:
            cassandra_type = attribute.cassandra_type
            if cassandra_type is not None:
                regular_attributes.append((attribute, cassandra_type))

    # Create regular table, if any regular attributes.
    if regular_attributes:
        texts = []
        key_attribute = entity_class.get_key_attribute()
        for attribute, cassandra_type in regular_attributes:
            if attribute.is_set_like and attribute.enum:
                continue
            attribute_name = attribute.name
            # Our CQL gets mussed if we try to use a column name 'token'
            if attribute_name == 'token':
                attribute_name = 'hsn_token'
#            if attribute == key_attribute:
#                texts.append('%s %s PRIMARY KEY' % (attribute_name, cassandra_type))
#            else:
            # Note: I took out the above three lines when moving to a
            # version that can handle composite keys.  I'm leaving it in until
            # this passes tests for single-key column families.
            texts.append('%s %s' % (attribute_name, cassandra_type))
        texts.append('PRIMARY KEY(%s)' % ', '.join(index_attrs))
        statement = 'CREATE COLUMNFAMILY %s (hsn_deleted boolean, %s);' % (
            scalar_table_name,
            ', '.join(texts))
        execute(statement,
                consistency_level=settings.BACKEND_MIGRATION_CONSISTENCY,
                connection=connection, cursor=cursor)

    if counter_attributes and index is None:
        counter_table_name = entity_class.get_counter_column_family()
        key_attribute = entity_class.get_key_attribute()
        statement = 'CREATE COLUMNFAMILY %s (%s %s PRIMARY KEY, %s)' % (
            counter_table_name,
            key_attribute.name,
            key_attribute.cassandra_type,
            ', '.join(['%s counter' % c.name for c in counter_attributes]))
        execute(statement,
                consistency_level=settings.BACKEND_MIGRATION_CONSISTENCY,
                connection=connection, cursor=cursor)

def cassandra_drop_column_families(entity_class, suppress_errors=False):
    """Remove column families from Cassandra for a given Entity class."""
    from backend.schema import ManyToMany, Incrementer
    has_counter_attributes = False
    has_regular_attributes = False

    for a in entity_class.attributes:
        attribute = entity_class.get_attribute(a)
        if isinstance(attribute, ManyToMany):
            raise NotImplementedError('%s not supported' % attribute)
        elif isinstance(attribute, Incrementer):
            has_counter_attributes = True
        else:
            has_regular_attributes = True
        if has_counter_attributes and has_regular_attributes:
            break

    if has_regular_attributes:
        cassandra_table_name = entity_class.get_primary_column_family()
        statement = 'DROP COLUMNFAMILY %s' % cassandra_table_name
        try:
            if suppress_errors:
                execute(statement, retries=0,
                        consistency_level=settings.BACKEND_MIGRATION_CONSISTENCY)
            else:
                execute(statement,
                        consistency_level=settings.BACKEND_MIGRATION_CONSISTENCY)
        except:
            if not suppress_errors:
                raise
        for index, attributes in entity_class.persistent_class.indexes.iteritems():
            statement = 'DROP COLUMNFAMILY %s' % (
                entity_class.get_index_column_family(index))
            try:
                if suppress_errors:
                    execute(statement, retries=0,
                            consistency_level=settings.BACKEND_MIGRATION_CONSISTENCY)
                else:
                    execute(statement,
                            consistency_level=settings.BACKEND_MIGRATION_CONSISTENCY)
            except:
                if not suppress_errors:
                    raise

    if has_counter_attributes:
        cassandra_table_name = entity_class.get_counter_column_family()
        statement = 'DROP COLUMNFAMILY %s' % cassandra_table_name
        try:
            if suppress_errors:
                execute(statement, retries=0,
                        consistency_level=settings.BACKEND_MIGRATION_CONSISTENCY)
            else:
                execute(statement,
                        consistency_level=settings.BACKEND_MIGRATION_CONSISTENCY)
        except:
            if not suppress_errors:
                raise


def _cassandra_drop_columnfamily(entity_class, index=None, suppress_errors=False):
    cassandra_table_name = entity_class.get_primary_column_family()
    if index:
        cassandra_table_name = '%s_%s' % (cassandra_table_name, index)

    statement = 'DROP COLUMNFAMILY %s' % (cassandra_table_name)
    try:
        if suppress_errors:
            execute(statement, retries=0,
                    consistency_level=settings.BACKEND_MIGRATION_CONSISTENCY)
        else:
            execute(statement,
                    consistency_level=settings.BACKEND_MIGRATION_CONSISTENCY)
    except:
        if not suppress_errors:
            raise


def cassandra_add_column(entity_class, attribute_name, suppress_errors=False):
    column_families = []
    from backend.schema import Incrementer
    attribute = entity_class.get_attribute(attribute_name)
    cassandra_type = attribute.cassandra_type
    if isinstance(attribute, Incrementer):
        column_families.append(entity_class.get_counter_column_family())
    else:
        column_families.append(entity_class.get_primary_column_family())
        for index in entity_class.persistent_class.indexes.keys():
            column_families.append(entity_class.get_index_column_family(index))

    for column_family in column_families:
        if suppress_errors:
            try:
                util.cassandra.add_column(column_family,
                                          attribute_name,
                                          cassandra_type,
                                          retries=0)
            except Exception:
                pass
        else:
            util.cassandra.add_column(column_family,
                                      attribute_name,
                                      cassandra_type)


def cassandra_drop_column(entity_class, attribute_name, suppress_errors=False):
    column_families = []
    from backend.schema import Incrementer
    attribute = entity_class.get_attribute(attribute_name)
    if isinstance(attribute, Incrementer):
        column_families.append(entity_class.get_counter_column_family())
    else:
        column_families.append(entity_class.get_primary_column_family())
        for index in entity_class.persistent_class.indexes.keys():
            column_families.append(entity_class.get_index_column_family(index))

    for column_family in column_families:
        if suppress_errors:
            try:
                util.cassandra.drop_column(column_family,
                                           attribute_name,
                                           retries=0)
            except Exception:
                pass
        else:
            util.cassandra.drop_column(column_family, attribute_name)

GET_VERSION_FETCHER = Fetcher(cf_name='b__migration_version', fields=('major', 'minor', 'micro'), relations=(ColumnValueRelation(column_name='key', literal='hsn'),), consistency_level='QUORUM')

def cassandra_get_version():
    global GET_VERSION_FETCHER
    try:
        result = GET_VERSION_FETCHER.fetch_first()
    except ProgrammingError, e:
        if e.args == ('Bad Request: unconfigured columnfamily b__migration_version',):
            return
        if e.args == ("ProgrammingError: Bad Request: Keyspace %s does not exist" % settings.DSE_CASSANDRA_KEYSPACE,):
            return
        logger.exception('Cannot get version from Cassandra with query %s, got %s', GET_VERSION_FETCHER, result)
        return
    if result is None:
        logger.error('No version info in Cassandra (is this an empty install?) with query %s', GET_VERSION_FETCHER)
        return
    try:
        major, minor, micro = result
    except Exception:
        logger.exception('Can not get version from Cassandra, got %s', result)
        return
    return major, minor, micro

def cassandra_set_version(major, minor, micro, connection=None, cursor=None):
    batch(cfs_data_pairs={
        ('b__migration_version', ('key',)): [
            {'key': 'hsn',
             'major': major,
             'minor': minor,
             'micro': micro}]},
          consistency_level=settings.BACKEND_MIGRATION_CONSISTENCY,
          connection=connection, cursor=cursor)
    logger.debug('Cassandra migration version set to %s %s %s', major, minor, micro)

def cassandra_model_diff(collection=None):
    if collection is None:
        from model.imports import ENTITIES
        collection = ENTITIES
    schema = {}  # entity_class => (set(attrs), set(incrementers))
    from backend.schema import ManyToMany, Incrementer
    for persistent_class in collection:
        entity_class = persistent_class.entity_class
        counter_attributes = []
        regular_attributes = []
        for a in entity_class.attributes:
            attribute = entity_class.get_attribute(a)
            if isinstance(attribute, ManyToMany):
                raise NotImplementedError('%s not supported' % attribute)
            elif isinstance(attribute, Incrementer):
                counter_attributes.append(attribute)
            else:
                cassandra_type = attribute.cassandra_type
                if cassandra_type is not None:
                    regular_attributes.append((attribute, cassandra_type))

        if counter_attributes:
            column_family = entity_class.get_counter_column_family()
            pass

        if regular_attributes:
            pass

        schema[persistent_class.entity_class] = (regular_attributes,
                                                 counter_attributes)

    # Below this section of code is a comparer that will compare these three
    # datastructures with the 'schema' structure above.
    from util.cassandra import get_column_families, get_keys_by_column_family, get_values_by_column_family
    keyspace = settings.DSE_CASSANDRA_KEYSPACE
    column_families_found = set(get_column_families(keyspace)[keyspace])
    column_family_to_key_attribute_type = dict(get_keys_by_column_family(keyspace))
    by_column_family = dict(get_values_by_column_family(keyspace))

    # These are some private functions to aid reading the cassandra definitions.
    def _metadata_to_type(s):
        return {
            'org.apache.cassandra.db.marshal.UUIDType': UUID,
            'org.apache.cassandra.db.marshal.DoubleType': float,
            'org.apache.cassandra.db.marshal.UTF8Type': unicode,
            'org.apache.cassandra.db.marshal.BooleanType': bool,
            'org.apache.cassandra.db.marshal.Int32Type': int,
            'org.apache.cassandra.db.marshal.LongType': long,
            'org.apache.cassandra.db.marshal.DateType': date
        }.get(s, s)

    def _type_to_metadata(s):
        return {
            'int': 'org.apache.cassandra.db.marshal.Int32Type',
            'text': 'org.apache.cassandra.db.marshal.UTF8Type',
            'uuid': 'org.apache.cassandra.db.marshal.UUIDType',
            UUID: 'org.apache.cassandra.db.marshal.UUIDType',
            float: 'org.apache.cassandra.db.marshal.DoubleType',
            'double': 'org.apache.cassandra.db.marshal.DoubleType',
            unicode: 'org.apache.cassandra.db.marshal.UTF8Type',
            'boolean': 'org.apache.cassandra.db.marshal.BooleanType',
            bool: 'org.apache.cassandra.db.marshal.BooleanType',
            int: 'org.apache.cassandra.db.marshal.Int32Type',
            long: 'org.apache.cassandra.db.marshal.LongType',
            'bigint': 'org.apache.cassandra.db.marshal.LongType',
            date: 'org.apache.cassandra.db.marshal.DateType',
            'decimal': 'org.apache.cassandra.db.marshal.DecimalType'
        }.get(s, s)

    # Compare the schema we have to Cassandra.
    errors = []
    legal_column_families = set()  # Accounted for column families.
    for entity_class, (regular_attributes, counter_attributes) in schema.iteritems():
        key_attr = entity_class.get_key_attribute()
        column_family = entity_class.get_primary_column_family()
        index_families = [entity_class.get_index_column_family(index) for index in entity_class.persistent_class.indexes.keys()]
        counter_column_family = entity_class.get_counter_column_family()
        for cf in [column_family, counter_column_family] + index_families:
            legal_column_families.add(cf)
        if not entity_class.persistent_class.saves_to_cassandra:
            if column_family in by_column_family:
                logger.info('%s does not save to Cassandra but has %s column family in Cassandra', entity_class.__name__, column_family)
            if counter_column_family in by_column_family:
                logger.info('%s does not save to Cassandra but has %s column family in Cassandra', entity_class.__name__, column_family)
        elif regular_attributes:
            found = set()
            if column_family not in by_column_family:
                errors.append('Cassandra does not have column family %s' % column_family)
                continue
            attributes = by_column_family[column_family]
            if 'hsn_deleted' in attributes:
                found.add('hsn_deleted')
            else:
                errors.append('Cassandra column family %s does not have column hsn_deleted' % column_family)

            for attribute, attr_type in regular_attributes:
                if attribute == key_attr:
                    if column_family not in column_family_to_key_attribute_type:
                        errors.append('Column Family %s did not list a key attribute type' % column_family)
                    else:
                        key_type = column_family_to_key_attribute_type[column_family][0][1]
                        if type(key_attr.field_instance).__name__ == 'TimeUUIDField':
                            local_key_type = 'org.apache.cassandra.db.marshal.TimeUUIDType'
                        else:
                            local_key_type = _type_to_metadata(key_attr.value_class)
                        if local_key_type != key_type:
                            errors.append('Column Family %s had key type %s, expected %s' % (column_family, key_type, local_key_type))
                    continue
                if attribute.name == 'token':
                    attrname = 'hsn_token'
                else:
                    attrname = attribute.name
                if attrname not in attributes:
                    if attribute.is_set_like and attribute.enum:
                        continue
                    errors.append('Cassandra column family %s does not have column %s, as required by %s.%s' % (column_family, attrname, entity_class.__name__, attribute.name))
                else:
                    found.add(attrname)
                    if _type_to_metadata(attr_type) != attributes[attrname]:
                        ignores = CASSANDRA_IGNORE_COLUMNS.get(column_family.lower(), set())
                        if attrname not in ignores:
                            errors.append('Cassandra %s.%s expected type %s, found %s' % (
                                column_family, attribute.name, attr_type, attributes[attribute.name]))
            for column, key_type in by_column_family[column_family].iteritems():
                if column not in found:
                    ignores = CASSANDRA_IGNORE_COLUMNS.get(column_family.lower(), set())
                    if column not in ignores:
                        errors.append('Cassandra column family %s had column %s, which is not in the model' % (column_family, column))
            for cfam in index_families:
                #if cfam not in by_column_family:
                if cfam.lower() not in column_family_to_key_attribute_type:
                    errors.append('Cassandra does not have column family %s, as required by %s' % (cfam, entity_class.__name__))

        if counter_attributes:
            if counter_column_family not in by_column_family:
                errors.append('Cassandra does not have column family %s, as required by %s' % (counter_column_family, entity_class.__name__))
            else:
                found = set()
                attributes = by_column_family[counter_column_family]
                for attribute in counter_attributes:
                    if attribute.name not in attributes:
                        errors.append('Cassandra column family %s does not have column %s, as required by %s.%s' % (counter_column_family, attribute.name, entity_class.__name__, attribute.name))
                    else:
                        found.add(attribute.name)
                        if 'org.apache.cassandra.db.marshal.CounterColumnType' != attributes[attribute.name]:
                            errors.append('Cassandra %s.%s expected counter type, found %s' % (
                                counter_column_family, attribute.name, attributes[attribute.name]))
                for column, key_type in by_column_family[counter_column_family].iteritems():
                    if column not in found:
                        errors.append('Cassandra column family %s had column %s, which is not in the model' % (column_family, column))

    # Look at what we found that was not expected.
    legals = legal_column_families | CASSANDRA_VERIFY
    for column_family in column_families_found:
        if column_family not in legals:
            errors.append('Cassandra had unexpected column family %s' % column_family)
    return errors

# Set of column families expected to be found in Cassandra.
# (schema-derived column families do not need to be listed here.)
CASSANDRA_VERIFY = set([
    'b__migration_version',
    'collection_action',
    'other_action',
    'post_action',
    'post_action_renderable',
    'product_request_action',
    'project_action',
    'project_stats',
    'purchase_action',
    'session',
    'session_key_action',
    'storm_collection_action',
    'storm_other_action',
    'storm_post_action',
    'storm_post_action_renderable',
    'storm_product_request_action',
    'storm_project_action',
    'storm_purchase_action',
    'storm_session_key_action',
    'storm_user_action',
    'storm_user_action_renderable',
    'user',
    'user_action',
    'user_action_renderable',
    'user_counts',
    'user_edges',
    'user_friends',
    'user_notifications',
    'user_session',
    'content_porosity',
    'cache_counter',
    'shortened_url_action',
    'project_subscription_action',
    'storm_project_subscription_action',
    'storm_shortened_url_action'
])

# If a column family has these extra columns, ignore them.
CASSANDRA_IGNORE_COLUMNS = {
    # cf_name: set([col1, col2, ...]),
    'b_userdemographics': set(['created', 'fb_relationshiop_status']),

    #deprecated, to be dropped
    'b_bundledownload': set(['size']),
    'b_spamreport': set(['post']),
    'b_clubpreviewcontent': set(['content'])
}

def _get_migration_files():
    import os
    from django.conf import settings
    files = os.listdir(settings.CODE_DIR + '/hsn/model/versions/cassandra')
    files = [f for f in files if f.endswith('.py')]
    tups = []
    for f in files:
        try:
            nums = f[:11]
            name = f[12:][:-3]
            s = nums.split('_')
            major = int(s[0])
            minor = int(s[1])
            micro = int(s[2])
            tups.append(((major, minor, micro,), name, f))
        except:
            pass

    tups.sort()
    return tups

def cassandra_upgrade(version):
    """Upgrade cassandra up to and including the given version.

    version -- string must be of format major.minor.micro ex: 2.1.0

    If not version, upgrade to latest.

    """
    tups = _get_migration_files()

    # This represents the target version.
    if not version:
        logger.info('cassandra upgrade to latest')
        tmajor = None
        tminor = None
        tmicro = None
    else:
        try:
            tmajor, tminor, tmicro = version.split('.')
            tmajor = int(tmajor)
            tminor = int(tminor)
            tmicro = int(tmicro)
            logger.info('cassandra upgrade to %s.%s.%s', tmajor, tminor, tmicro)
        except Exception:
            logger.info("version argument '%s' is invalid. Must be blank for latest, or a complete version ex: 2.1.0", version)
            return 1
        # Check for invalid version.
        found = False
        for (major, minor, micro), name, filename in tups:
            if (major, minor, micro) == (tmajor, tminor, tmicro):
                found = True
                break
        if not found:
            logger.info('version %s.%s.%s not found, aborting', tmajor, tminor, tmicro)
            return 1

    # This represents the current version.
    try:
        cmajor, cminor, cmicro = cassandra_get_version()
    except Exception:
        logger.info('version info not found in cassandra, aborting.')
        return 1

    logger.info('cassandra @ %s.%s.%s', cmajor, cminor, cmicro)

    # Import and run the upgrade from the appropriate migration files.
    from django.conf import settings
    import imp
    for (major, minor, micro), name, filename in tups:
        # Skip any file earlier or equal to the current version.
        if (major, minor, micro) <= (cmajor, cminor, cmicro):
            continue
        # Skip any file greater than the target version, if any.
        if tmajor is not None:
            if major > tmajor:
                continue
            if tminor is not None:
                if minor > tminor:
                    continue
                if tmicro is not None:
                    if micro > tmicro:
                        continue
        logger.info('running upgrade %s.%s.%s - %s', major, minor, micro, name)
        path = settings.CODE_DIR + '/hsn/model/versions/cassandra/' + filename
        migration = imp.load_source(name, path)
        migration.upgrade()
        cassandra_set_version(major, minor, micro)
        cmajor = major
        cminor = minor
        cmicro = micro
        
def cassandra_downgrade(version):
    """Downgrade cassandra to the given version.

    version -- string must be of format major.minor.micro ex: 2.1.0

    Note that, for example, if you had most recently run migration 4.6.2 and
    then ran cassandra_downgrade('4.6.0') the 'downgrade' functions in the
    4.6.2 and 4.6.1 files would be run.  This leaves Cassandra in the state
    defined by all of the migrations ('upgrade' functions) up to and
    including 4.6.0

    """
    # This represents the target version.
    try:
        tmajor, tminor, tmicro = version.split('.')
        tmajor = int(tmajor)
        tminor = int(tminor)
        tmicro = int(tmicro)
    except Exception:
        logger.info("version argument '%s' is invalid. Must be complete version ex: 2.1.0", version)
        return 1

    tups = _get_migration_files()
    tups.reverse()

    # Check for invalid version.
    if version != '0.0.0':
        found = False
        for (major, minor, micro), name, filename in tups:
            if (major, minor, micro) == (tmajor, tminor, tmicro):
                found = True
                break
        if not found:
            logger.info('version %s.%s.%s not found, aborting', tmajor, tminor, tmicro)
            return 1

    try:
        cmajor, cminor, cmicro = cassandra_get_version()
    except Exception:
        cmajor = 0
        cminor = 0
        cmicro = 0

    logger.info('cassandra @ %s.%s.%s', cmajor, cminor, cmicro)

    logger.info('downgrade to %s.%s.%s', tmajor, tminor, tmicro)

    import imp
    from django.conf import settings
    is_first = True
    for (major, minor, micro), name, filename in tups:
        # Skip any file more recent than the current version.
        if (major, minor, micro) > (cmajor, cminor, cmicro):
            continue

        if is_first:
            is_first = False
        else:
            # Set the version that the last iteration moved us into.
            cassandra_set_version(major, minor, micro)

        if (major, minor, micro) == (tmajor, tminor, tmicro):
            break

        logger.info('running downgrade %s.%s.%s - %s', major, minor, micro, name)
        path = settings.CODE_DIR + '/hsn/model/versions/cassandra/' + filename
        migration = imp.load_source(name, path)
        migration.downgrade()

def populate_cassandra_index(entity_class, index_name):
    """Copy data into new Cassandra index column family.

    This copies all of the data from the primary column family to the new
    index column family so one might expect this to be a slow operation.

    If calling this from within a migration be sure to reference the model's
    version of the persistent_class as the migration ususally only has an
    abbreviated schema.

    Note: The column family should already be created.

    """
    if entity_class.key_attribute_name == 'token':
        raise NotImplementedError("Entities with key named 'token' not supported")
    primary_column_family = entity_class.get_primary_column_family()
    index_column_family = entity_class.get_index_column_family(index_name)
    attrs = []
    from backend.schema import OneToOne, OneToMany, ManyToMany, Incrementer
    for a in entity_class.attributes:
        attribute = entity_class.get_attribute(a)
        if isinstance(attribute, ManyToMany):
            raise NotImplementedError('%s not supported' % attribute)
        elif isinstance(attribute, (Incrementer, OneToMany, OneToOne, )):
            pass
        else:
            cassandra_type = attribute.cassandra_type
            if cassandra_type is not None:
                attrs.append((attribute, cassandra_type))

    # Get a cursor that will select all of these values from the primary
    # column family.
    attr_names = tuple(a[0].name for a in attrs)
    primary_column_family_fetcher = Fetcher(cf_name=primary_column_family, fields=attr_names)
    key_attr_names = entity_class.persistent_class.indexes[index_name]

    for result in primary_column_family_fetcher.fetch_all():
        values = {'hsn_deleted': False}
        for result, attr_name in izip(result, attr_names):
            values[attr_name] = result

        #check for key values.  they can't be none or missing
        has_all_keys = True
        for k in key_attr_names:
            key_value = values.get(k, None)
            if key_value is None or key_value == '':
                has_all_keys = False
                break

        if not has_all_keys:
            continue

        batch(cfs_data_pairs={
            (index_column_family, key_attr_names): [values]})
#        # For each row, create an insert statement for the index column family.
#        stmt = u"INSERT INTO %s (%s, hsn_deleted) VALUES (%s, :hsn_deleted)\n" % (
#            index_column_family, namestr, keynamestr)
#        values['hsn_deleted'] = False
#        execute(stmt, values)

def eliminate_cassandra_enum_nones(entity_class, dry_run=False):
    # xxx this does not work on large dataset like the one found in production.
    # Note: Nothing with 'token' as an attribute name applies here.
    # Note: Nothing with an enum as a key in in index applies here.
    # The only entity_class with enums and indexes is probably videotranscode,
    # which may not exhibit this problem due to its short lived nature (which
    # I assume, but am not sure about.)  We will support indexes, but it
    # might be OK if it doesn't happen.
    # write something that detects None in enum fields, in both cassandra and
    # postgresql.  if it finds a none it looks up the value in the other
    # storage if applicable and writes it, if it finds nothing it writes
    # attribute.default to both, unless the default is None, in which case it
    # writes Enum[0] (based on the assumption that up until now, that's what
    # the "default" was.)
    logger.info('eliminating enum Nones for %s', entity_class.__name__)
    primary_column_family = entity_class.get_primary_column_family()
    enum_attrs = []
    for a in entity_class.attributes:
        attribute = entity_class.get_attribute(a)
        if attribute.enum and not attribute.is_set_like:
            enum_attrs.append(attribute)
    if not enum_attrs:
        #logger.info("%s does not have Enums, nothing to do", entity_class)
        return
    if entity_class.key_attribute_name == 'token':
        logger.info('not operating on %s because key_attribute_name is token', entity_class)
        raise NotImplementedError("Entities with key named 'token' not supported")
    if entity_class.persistent_class.indexes:
        logger.info('%s has indexes', entity_class)
    index_attrs = set()
    for index, index_attribute_names in entity_class.persistent_class.indexes.iteritems():
        index_attrs = index_attrs | set([entity_class.get_attribute(a) for a in index_attribute_names])
    for attribute in index_attrs:
        if attribute.enum:
            logger.info('index enum %s %s', entity_class, attribute)
            raise NotImplementedError
    for attribute in entity_class.attributes:
        if entity_class.get_attribute(attribute).name == 'token':
            raise NotImplementedError
    # At this point we have the enum attrs, the index attrs, and we've
    # already quit if we don't have enums.  Let's loop over this entity
    # class's row and find the entries that are missing.  If we find a miss
    # only then will we look into fixing the indexes.
    attrs = enum_attrs[:]
    # Make user the primary key comes first.
    attrs.insert(0, entity_class.get_key_attribute())
    progress_logger_checked = ProgressLogger(tickname='row')
    progress_logger_changed = ProgressLogger(tickname='None enum')
    # Get a cursor that will loop over every row in the cf.
    field_names = tuple(a.name for a in attrs)
    entity_fetcher = Fetcher(cf_name=primary_column_family, fields=field_names)
    for result in entity_fetcher.fetch_all():
        progress_logger_checked.tick()
        key = result[0]
        none_attrs = []
        for attribute, value in izip(attrs[1:], result[1:]):
            if value is None or value == '':
                progress_logger_changed.tick()
                none_attrs.append(attribute)
        if not none_attrs:
            # If we did not find any Nones, move on to the next row.
            continue
        # There were some Nones detected, attempt to load the from
        # postgresql, if this is a postgresql-writing entity.
        none_values = []
        if entity_class.persistent_class.saves_to_postgresql:
            # Look these up in postgresql.
            # Note we deal with the encoded form of an Enum, which is
            # just an int.  postgresql will return Enums.
            persistent = entity_class.persistent_class.get_by(**{entity_class.key_attribute_name: key})
            if persistent:
                for attr in none_attrs:
                    found_val = getattr(persistent, attr.name)
                    if found_val is not None:
                        none_values.append(found_val.number)
                    elif attr.default is not None:
                        none_values.append(attr.default.number)
                    else:
                        none_values.append(0)
                remove_session()  # to clear out the sql session.
            else:
                for attr in none_attrs:
                    if attr.default is not None:
                        none_values.append(attr.default.number)
                    else:
                        none_values.append(0)
        else:
            for attr in none_attrs:
                if attr.default is not None:
                    none_values.append(attr.default.number)
                else:
                    none_values.append(0)
        # Does this entity_class have indexes?  if so, we need to load
        # all the data and write to all valid index cfs.
        if entity_class.persistent_class.indexes:
            logger.warn('TODO need to write to indexes %s %s', entity_class, key)
        # Write the values to Cassandra.
        values = {'hsn_deleted': False,
                  entity_class.key_attribute_name: key}
        for none_attr, none_value in izip(none_attrs, none_values):
            values[none_attr.name] = none_value
        if not dry_run:
            batch(cfs_data_pairs={
                (primary_column_family, (entity_class.key_attribute_name,)): [values]})
    progress_logger_checked.finish()
    progress_logger_changed.finish()

def init_backend():
    """Initialize the backend for usage."""
    # Set up Elixir (Sql Alchemy) objects.
    # The session needs to be properly configured before setup_all().
    logger.debug("Backend -- Initializing Backend from %s", _repository)
    import util.database

    elixir.metadata.bind = util.database.db_url()
    elixir.metadata.bind.echo = False  # True for SQL debug info.

    # Create a configured 'Session' class.
    # (defaults are autoflush=True, autoexpire=True, autocommit=False)
    from sqlalchemy.orm import sessionmaker, scoped_session
    Session = scoped_session(sessionmaker(bind=elixir.metadata.bind))
    # Pass sessionmaker the argument bind=elixir.metadata.bind if you want to
    # run raw sql commands with get_session().execute('SELECT *... and have
    # them be part of the session's transaction.

    # Set the elixir session to be our configured Session class object.
    elixir.session = Session

    # import all model declarations. They are elixir objects.
    # TODO: Take the location of the model as an argument to init_backend.
    # Or load it from settings. (or I suppose in this case, a known location.)
    import model.imports
    entities = elixir.entities

    setupables = [e for e in entities if e.saves_to_postgresql]
    elixir.setup_entities(setupables)

    #elixir.setup_all()
    logger.debug("Backend - Elixir ORM setup complete.")

    setup_backend_mapping()
    setup_cache_inventory()
    setup_loader_inventory()

def setup_backend_mapping(collection=None):

    # If no collection is specified use the default.
    if not collection:
        collection = elixir.entities

    # This is the list of all known elixir.Entity objects.
    for persistent in collection:
        # Each is actuall a util.backend.Persistent subclass.
        # Set up the util.backend.Entity's Attributes.
        persistent.entity_class._setup_attributes()
        # this does not set up attribute.related_attribute, we do that later.
    
    logger.debug("Attribute setup complete for all Entity classes.")

    from backend.schema import (OneToOne, OneToOneField, ManyToOneField,
        OneToManyField, ManyToManyField, FakeManyToOneField, OneToNPManyField,
        ManyToNPOneUUIDField, FakeUUIDManyToNPOneField, OneToNPOneField)

    # Set up related attributes.
    for persistent in collection:
        fields = persistent.persistent_class.fields()
        # Set up related attributes for Elixir field based attributes.
        for field in fields:
            attribute = persistent.entity_class.attributes[field.name]
            assert(attribute is not None)
            if attribute.name == 'token':
                if persistent.__name__ not in ['LoginTokenPersist', 'OAuthTokenPersist', 'SQLLoginTokenPersist']:
                    raise IllegalSchema("'token' is not a valid field name (due to conflict with Cassandra keyword)")
            if hasattr(field, 'inverse'):
                if isinstance(field, (OneToNPManyField, ManyToNPOneUUIDField, FakeUUIDManyToNPOneField, OneToNPOneField)):
                    # In this case we just have the key, there is no
                    # SQL reverse relationship.
                    # But we still want to set up the related attribute.
                    related_field_name = field.inverse
                    related_class_name = field.related_persistent_class_name
                    found = None
                    for p in collection:
                        if p.__name__ == related_class_name:
                            found = p
                            break
                    if not found:
                        found = get_class(related_class_name)
                    related_persistent_class = found
                else:
                    related_field_name = field.inverse.name
                    related_persistent_class = field.related_persistent_class
                if not related_field_name:
                    assert(not isinstance(field,
                        (OneToOneField, OneToManyField, ManyToOneField,
                         ManyToManyField, FakeManyToOneField,
                         OneToNPManyField, ManyToNPOneUUIDField,
                         FakeUUIDManyToNPOneField, OneToNPOneField)))
                related_attribute = related_persistent_class.entity_class.attributes[related_field_name]
                assert(related_attribute is not None)
                attribute.related_attribute = related_attribute
                attribute.value_class = related_persistent_class.entity_class
                related_attribute.related_attribute = attribute
                if not attribute.is_set_like and not isinstance(attribute, OneToOne):
                    attribute.cassandra_type = attribute.value_class.get_key_attribute().cassandra_type
#                # Set up some needed special details.
                if isinstance(field, (ManyToOneField, FakeManyToOneField)):
#                    # If we change the naming convention in elixir we need to
#                    # change this code. See how ManyToMany saves formatter.
                    entity_class = related_attribute.entity_class
                    sql_key = entity_class.sql_primary_key_attribute.name
                    attribute.column_name = '%s_%s' % (field.name, sql_key)
                elif isinstance(field, ManyToManyField):
                    raise NotImplementedError
                    attribute.join_table_name = field.table.name
                    attribute.column_format = field.column_format

#        # and this is what we used to do for graphdb queries
#        # Set up related attributes for graphdb Query attributes.
#        queries = persistent.entity_class.queries
#        for query_name, query_args in queries.iteritems():
#            related_entity_class_name = query_args[1]
#            related_query_name = query_args[2]
#            attribute = persistent.entity_class.attributes[query_name]
#            related_entity_class = get_class(related_entity_class_name)
#            related_attribute = related_entity_class.attributes[related_query_name]
#            attribute.related_attribute = related_attribute
#            related_attribute.related_attribute = attribute
#        # ---

    # Confirm all relationships are set up properly.
    found_a_bad_one = False
    for persistent in collection:
        for name, attribute in persistent.entity_class.attributes.iteritems():
            assert name
            assert attribute
            from backend.schema import OneToOne, OneToMany, ManyToOne, ManyToMany, FakeManyToOne
            if type(attribute) in [OneToOne, OneToMany, ManyToOne, ManyToMany, FakeManyToOne]:
                if attribute.related_attribute is None:
                    logger.warn("Oops! Attribute %s %s does not have a related_attribute", attribute.entity_class, attribute.name)
                    found_a_bad_one = True
    if found_a_bad_one:
        assert(False)

    # Check for illegal attributes in composite keys.
    for persistent in collection:
        if persistent.indexes:
            for index, attrs in persistent.indexes.iteritems():
                for attr in attrs:
                    if attr == 'token':
                        raise IllegalSchema("'token' may not be used in composite key indexes")

    # Populate attribute.indexes.  (Add index data to all attributes.)
    for persistent in collection:
        if persistent.indexes:
            for index, attrnames in persistent.indexes.iteritems():
                for attrname in attrnames:
                    a = persistent.entity_class.get_attribute(attrname)
                    a.indexes.add(index)

    # Populate the global name to class map.
    global _name_to_entity_class
    for persistent in collection:
        entity_class = persistent.entity_class
        _name_to_entity_class[entity_class.__name__] = entity_class

    # Prepare the default environment for first use.
    new_active_session()

def setup_cache_inventory(caches=None):
    if caches is None:
        from model.caches import CACHES
        caches = CACHES
    for cache in caches:
        cache.setup_inventory()

def setup_loader_inventory(loaders=None):
    if loaders is None:
        from model.caches import LOADERS
        loaders = LOADERS
    for loader in loaders:
        loader.setup_inventory()

# -- Migrations ---------------------------------------------------------------
# These functions are for managing database upgrades.

def postgresql_create():
    """Create the model's tables in the database."""
    # Create all of the tables for any declared (and imported) Persistent
    # object.

    # On new installs, the DB might be still initing when this is called.
    retries_left = 5
    while True:
        retries_left -= 1
        try:
            elixir.create_all()
            break
        except OperationalError, e:
            if retries_left > 0:
                logger.debug("Unable to call elixir.create_all; trying again in 5 seconds (%s tries left)", retries_left)
                sleep(5)
            else:
                raise e
        except Exception, e:
            logger.debug("Unknown error when calling elixir.create_all")
            raise e

    commit_session()
    clear_session()

    # Create the tables used for managing elixir migrations.
    logger.debug('Create Version/Migration Table')
    from migrate.versioning.api import version_control
    from model import migration_version
    import util.database
    version_control(util.database.db_url(),
                    _repository,
                    version=migration_version)

    logger.debug('Calling post-create hooks')
    from model import post_create
    post_create.post_create_hooks()

    logger.info("postgresql_create done")

def postgresql_upgrade(version=None):
    """Upgrade the database using the migration system."""
    if version is None:
        logger.info('postgresql upgrade to latest')
    else:
        logger.info('postgresql upgrade to %s', version)
    from migrate.versioning.api import upgrade
    import util.database
    upgrade(util.database.db_url(), _repository, version)
    logger.info("postgresql upgrade complete")

def postgresql_downgrade(version):
    """Downgrade the database using the migration system."""
    logger.debug("downgrade(%s)", version)
    from migrate.versioning.api import downgrade
    import util.database
    downgrade(util.database.db_url(), _repository, version)
    logger.debug("downgrade complete")

def postgresql_get_version():
    """Returns the migration version number found in postgresql."""
    from migrate.versioning.api import db_version
    from migrate.exceptions import DatabaseNotControlledError
    import util.database
    try:
        ver = db_version(util.database.db_url(), _repository)
    except DatabaseNotControlledError:
        ver = None
    return ver

def postgresql_model_diff():
    """Return the difference between the current model and postgresql."""
    from backend import schemadiff
    from sqlalchemy.exc import OperationalError
    try:
        diff = schemadiff.SchemaDiff(
            elixir.metadata,
            sqlalchemy.MetaData(elixir.metadata.bind, reflect=True),
            labelA='model',
            labelB='database',
            verify_tables=POSTGRESQL_VERIFY,
            table_verify=POSTGRESQL_TABLE_VERIFY)
    except OperationalError, e:
        diff = {'everything': e}
    return diff

# If a table is in verify tables it is expected to be in the db.
# It is irrelevant if it is in the model, but expected to not be
# in the model.
POSTGRESQL_VERIFY = [
    'apscheduler_jobs',
    'site_stats_raw',
    'migrate_version'
]

# If a model table is expected to have certain extra columns, put them here.
POSTGRESQL_TABLE_VERIFY = {
    'trending_videos': [
        'neg_log_p',
        'neg_log_p_time',
        'shares',
        'shares_time',
        'twitter_sharers']
}

def postgresql_initialize_attribute(entity_class, attribute_name,
                                    batch_size=300, wait=500,
                                    cassandra=False,
                                    cassandra_check_exists=False,
                                    dry_run=False,
                                    has_forced_value=False, forced_value=None):
    """Set default value on all rows where attribute value is None.

    entity_class -- Class whose postgresql table will updated
    attribute_name -- Name of attribute to initialize
    batch_size -- Do this many rows per batch
    wait -- Pause this many milliseconds between batches
    cassandra -- if True will also set the default in Cassandra
    cassandra_check_exists -- Does not write to Cassandra if row does not
                              already exist in Cassandra.
    dry_run -- if True nothing will be written to postgresql or cassandra

    """
    attribute = entity_class.get_attribute(attribute_name)
    if attribute.default is None and not has_forced_value or (has_forced_value and forced_value is None):
        logger.error("Called postgresql_initialize_attribute with %s's attribute %s, which has a default of None.", entity_class.__name__, attribute_name)
        return
    if has_forced_value:
        new_value = forced_value
    else:
        new_value = attribute.default
    from backend.environments.cassandra import Cassandra
    from backend.environments.deltas import SetAttribute
    from time import sleep

    def getter(cursor):
        while True:
            next = cursor.fetchmany(batch_size)
            if next:
                yield next
            else:
                return
    progress_logger = ProgressLogger(tickname='row')
    stmt = "SELECT %s FROM %s WHERE %s is null" % (
            entity_class.key_attribute_name,
            entity_class.persistent_class.table.name,
            attribute_name)
    with cursor(stmt) as postgresql_cursor:
        for group in getter(postgresql_cursor):
            # We are going to group up deltas and write to postgresql and
            # cassandra separately because we don't want to write to a Cassandra
            # row unless that row already exists in Cassandra.
            postgresql_deltas = []
            cassandra_deltas = []
            new_active_session()
            for key, in group:
                entity = entity_class(key)
                delta = SetAttribute(entity, attribute, new_value)
                postgresql_deltas.append(delta)
                if cassandra:
                    if cassandra_check_exists:
                        this_cassandra = Cassandra.exists(entity_class, key)
                    else:
                        this_cassandra = True
                else:
                    this_cassandra = False
                if this_cassandra:
                    # XXX must load sufficient data to write to indexes if needed.
                    # Note: This is a good time to think about certain sorts of
                    # generic loaders like AllLoader and IndexAttrbitues loader
                    cassandra_deltas.append(delta)
                progress_logger.tick()
            if not dry_run:
                # Do Cassandra deltas first because save will close the session.
                if cassandra_deltas:
                    new_active_session()
                    get_active_session().deltas = cassandra_deltas
                    save(main_persistent=False, try_later_on_this_thread_first=True)
                if postgresql_deltas:
                    new_active_session()
                    session = get_active_session()
                    session.accelerated_save = True
                    session.deltas = postgresql_deltas
                    save(cassandra=False, try_later_on_this_thread_first=True)
            sleep(wait / 1000.0)
    progress_logger.finish()
    

# -- The SQL ALchemy Session --------------------------------------------------
# Backend is built on SQL Alchemy and Elixir, both of which are much exposed
# in Backend. Because it is possible to use the SQL aspect of Backend without
# using the session, these are made public.

def get_session():
    """Return session."""
    return elixir.session

@retry_operational_error()
def flush_session():
    """Flush current session."""
    _reset_persists()
    return get_session().flush()

@retry_operational_error()
def commit_session():
    """Commit current session."""
    _reset_persists()
    return get_session().commit()

@retry_operational_error()
def close_session():
    """Close current SQL Alchemy session."""
    _reset_persists()
    return get_session().close()

@retry_operational_error()
def remove_session():
    """Remove current session."""
    _reset_persists()
    return get_session().remove()

@retry_operational_error()
def rollback_session():
    """Rollback current session"""
    logger.debug("Rollback Session")
    _reset_persists()
    return get_session().rollback()

@retry_operational_error()
def clear_session():
    """Clear current session."""
    logger.debug("Clear Session")
    _reset_persists()
    return get_session().expunge_all()

def _reset_persists():
    """Clear our locally-held identity map of SQA Instances, if exists."""
    from backend.environments.persistent import MainPersistent
    MainPersistent._reset_persists()

def _iter_subclasses(cls, _seen=None):
    if _seen is None: _seen = set()
    try:
        subs = cls.__subclasses__()
    except TypeError:  # fails only when cls is type
        subs = cls.__subclasses__(cls)
    for sub in subs:
        if isinstance(sub, type) and sub.persistent_class and sub not in _seen:
            _seen.add(sub)
            yield sub
            for sub in _iter_subclasses(sub, _seen):
                yield sub

class RepairStats(object):
    def __init__(self, logging=True, verbose=True, is_alive_timeout=60):
        self.logging = logging
        self.verbose = verbose
        if not isinstance(is_alive_timeout, timedelta):
            is_alive_timeout = timedelta(seconds=is_alive_timeout)
        self.is_alive_timeout = is_alive_timeout
        self.total_entities_read = 0
        self.last_entities_read = 0
        self.total_differences = 0
        self.last_differences = 0
        self.total_entities_repaired = 0
        self.last_entities_repaired = 0
        self.total_primary_rows_repaired = 0
        self.last_primary_rows_repaired = 0
        self.total_index_rows_repaired = 0
        self.last_index_rows_repaired = 0
        self.total_counter_rows_repaired = 0
        self.last_counter_rows_repaired = 0
        self.error_count = 0
        self.error_messages = []

    def _reset_entity_logger(self):
        if self.is_alive_timeout:
            prefix = '%s entit%s' % (self.name, '%s')
            self.entity_logger = ProgressLogger(tickname=prefix % 'y',
                                                tickplural=prefix % 'ies',
                                                timeout=self.is_alive_timeout)
        else:
            self.entity_logger = None

    def tick_entity_read(self):
        self.total_entities_read += 1
        self.last_entities_read += 1
        if self.is_alive_timeout:
            self.entity_logger.tick()

    def tick_difference(self):
        self.total_differences += 1
        self.last_differences += 1

    def tick_entities_repaired(self):
        self.total_entities_repaired += 1
        self.last_entities_repaired += 1

    def add_error(self, message):
        self.error_count += 1
        self.error_messages.append(message)

    def finish_logging_entity(self):
        entities = pluralize(self.last_entities_read, 'entity', 'entities')
        differences = pluralize(self.error_messages, 'difference')
        if self.logging:
            if self.error_messages:
                if self.verbose:
                    if logger.isEnabledFor(INFO):
                        logger.info('%s NOT OK; %s read, %s\n%s',
                                    self.name, entities, differences,
                                    '\n'.join(self.error_messages))
                else:
                    logger.info('%s NOT OK; %s read, %s', self.name, entities, differences)
            else:
                logger.info('%s OK %s read', self.name, entities)
        self.error_messages = []
        self.last_entities_read = 0

    def begin_logging_entity(self, new_name):
        self.name = new_name
        self._reset_entity_logger()

    def log_final(self, verb='verified'):
        if self.logging:
            if logger.isEnabledFor(INFO):
                logger.info('DONE %s %s',
                            pluralize(self.total_entities_read, 'entity', 'entities'),
                            verb)

def repair_cassandra_from_postgres(
        entity_classes=None,
        start_with_class=None,
        skip=None,
        random_count=None,
        statement=None,
        keys=None,
        overwrite=False,
        repair=False,
        verify=True,
        blank_equals_none=True,
        attributes=None,
        force=None,
        dry_run=False,
        logging=True,
        verbose=True,
        is_alive_timeout=60,
        temp=False,
        autocommit=False,
        set_timestamp=True):
    """Repair/Verify Cassandra data using data from Postgresql.

    entity_classes -- entity_class/list of them. model.imports.ENTITIES if None
    start_with_class -- Name of entity_class to start from in entity_classes.
    skip -- class, name or list therof.  Skip repair/verify of these.
    random_count -- If not None, get # random samples from each entity_class
                    NOT IMPLEMENTED
    statement -- If not None, use this SQL.  entity_class must be singular.
                 NOT TESTED
    keys -- If not None, use only these keys.  entity_class must be singular.
            NOT TESTED
    overwrite -- Write Postgresql data to Cassandra without reading Cassandra
    repair -- If False do not write to Cassandra
    verify -- If True print detailed logging on pre-repair discrepancies
    blank_equals_none -- If True, all operations will consider None == ''
    attributes -- If not None, only these attributes will be considered
                  NOT TESTED
    force -- If not None, a dict attrname, value to be set in Cassandra.
             if overwrite = False, will only force if existing Cassandra value
             is None or ''.
             NOT IMPLEMENTED
    dry_run -- Do not execute writes
    logging -- If True log general messages like which class is being verified.
    verbose -- If True log the differences discovered between pg and c
    is_alive_timeout -- seconds between progress logs, or None to forgo such.
    verbose -- If False, do not log verify/repair messages.
    temp -- If True, use a server-side temporary table
    autocommit -- If True cursor will be autocommit
    set_timestamp -- If True, writes are timestamped based on when the data was
                     read from PostgreSQL, so newer writes take precedent. If
                     False, writes are given the default timestamp (now)

    returns -- Number of errors that may still exist in the records examined.

    """
    if entity_classes is None:
        from model.imports import ENTITIES
        entity_classes = ENTITIES
    from backend.environments.core import Entity
    if not isinstance(entity_classes, list):
        try:
            is_entity = issubclass(entity_classes, Entity)
        except TypeError:
            is_entity = False
        if not is_entity:
            raise ValueError(
                'entity_classes must be Entity or list of Entities, was %s' %
                type(entity_classes))
    try:
        is_entity = issubclass(entity_classes, Entity)
    except:
        is_entity = False
    if is_entity:
        entity_classes = [entity_classes]
    if not entity_classes:
        raise ValueError("called with empty 'entity_classes'")
    entity_classes = [e for e in entity_classes if e.persistent_class.saves_to_postgresql and e.persistent_class.saves_to_cassandra]
    if not entity_classes:
        raise ValueError("None of the given entity classes save to both postgresql and cassandra")

    if skip:
        if not isinstance(skip, list):
            skip = [skip]
        skip_entities = set()
        for s in skip:
            try:
                is_entity = issubclass(s, Entity)
            except:
                is_entity = False
            if not is_entity:
                s = _name_to_entity_class[s]
            skip_entities.add(s)
        entity_classes = [e for e in entity_classes if e not in skip_entities]

    if overwrite and verify:
        raise ValueError("Can't have overwrite and verify both be True")
    if not overwrite and not verify and not repair:
        raise ValueError("Must either overwrite, verify or repair")
    if temp and statement:
        raise ValueError("Temp must be False if 'statement' is not None")

    if start_with_class is not None:
        new_entity_classes = []
        found = False
        skips = 0
        for entity_class in entity_classes:
            if entity_class.__name__ == start_with_class:
                found = True
            if found:
                new_entity_classes.append(entity_class)
            else:
                skips += 1
        if skips:
            skip_str = pluralize(skips, 'entity_class', '%ses')
            logger.info('skipping %s', skip_str)
        entity_classes = new_entity_classes

    if not entity_classes:
        raise ValueError("No entity classes to process")

    if overwrite:
        verb = 'overwritting'
    elif repair:
        verb = 'repairing'
    else:
        verb = 'verifying'
    if logging:
        if logger.isEnabledFor(INFO):
            logger.info('START repair_cassandra_from_postgres.  %s %s',
                        verb,
                        pluralize(entity_classes, 'entity_class', '%ses'))

    stats = RepairStats(logging=logging,
                        verbose=verbose,
                        is_alive_timeout=is_alive_timeout)
    unfixed_errors = 0
    for entity_class in entity_classes:
        stats.begin_logging_entity(entity_class.__name__)
        if logging:
            if not dry_run and overwrite:
                logger.info('Overwriting %s', entity_class.__name__)
            elif not dry_run and repair:
                logger.info('Repairing %s', entity_class.__name__)
            else:
                logger.info('Verifying %s', entity_class.__name__)
        try:
            unfixed_errors += _repair_cassandra_from_postgres_by_entity_class(
                entity_class,
                stats,
                #random_count=None,
                statement=statement,
                keys=keys,
                overwrite=overwrite,
                repair=repair,
                verify=verify,
                blank_equals_none=blank_equals_none,
                attributes=attributes,
                force=force,
                dry_run=dry_run,
                temp=temp,
                autocommit=autocommit,
                set_timestamp=set_timestamp)
        except (ProgrammingError, OperationalError), e:
            msg = "repair for %s raised exception %s" % (
                entity_class.__name__, e)
            stats.add_error(msg)
            unfixed_errors += 1
        except Exception, e:
            logger.exception('got exception %s on entity_class %s', e, entity_class)
            raise
        stats.finish_logging_entity()

    if overwrite:
        verb = 'overwritten'
    elif repair:
        verb = 'repaired'
    else:
        verb = 'verified'
    stats.log_final(verb=verb)
    return unfixed_errors

def _repair_cassandra_from_postgres_by_entity_class(
        entity_class,
        stats,
    #    random_count=None,
        statement=None,
        keys=None,
        overwrite=False,
        repair=False,
        verify=True,
        blank_equals_none=False,
        attributes=None,
        force=None,
        dry_run=False,
        temp=True,
        autocommit=False,
        set_timestamp=True):
    # Construct the statement we will get from postgresql.
    key_attribute_name = entity_class.key_attribute_name
    from backend.schema import Incrementer, OneToOne

    def attr_in_table(attr):
        """True if this attribute holds its value in its entity's own table."""
        if attr.is_set_like:
            return False
        if isinstance(attr, OneToOne):
            return False
        return True

    attrs = list(entity_class.attributes.values())
    if attributes:
        attrs = [a for a in attrs if a.name in attributes]
    attrs = [a for a in attrs if attr_in_table(a)]
    scalars = []
    incrs = []
    for a in attrs:
        if isinstance(a, Incrementer):
            incrs.append(a)
        else:
            scalars.append(a)

    attr_names = []
    for a in attrs:
        try:
            column_name = a.column_name
        except:
            column_name = a.name
        attr_names.append(column_name)

    if statement:
        stmt = statement
    elif temp:
        astr = ', '.join(['"%s"' % n for n in attr_names])
        base_select = "SELECT %s FROM %s" % (astr, '%s')
        tname = entity_class.persistent_class.table.name
        temp_tname = 'temp%s' % tname
        select = base_select % tname
        if keys:
            select += ' WHERE %s IN (%s)' % (key_attribute_name, ', '.join(keys))
        temp_select = base_select % temp_tname
        stmt = "CREATE TEMP TABLE %s ON COMMIT DROP AS %s;%s" % (
            temp_tname, select, temp_select)
    else:
        stmt = "SELECT %s FROM %s" % (', '.join(['"%s"' % n for n in attr_names]), entity_class.persistent_class.table.name)
        if keys:
            stmt += ' WHERE %s IN (%s)' % (key_attribute_name, ', '.join(keys))

    from util.database import cursor
    with cursor(stmt,
                autocommit=autocommit,
                with_time=set_timestamp) as postgresql_cursor:
        if set_timestamp:
            transaction_timestamp = postgresql_cursor.next()
        else:
            transaction_timestamp = None
        unfixed_errors = 0
        for record in postgresql_cursor:
            stats.tick_entity_read()
            # Make a new session to flush Entities that are instatiated as values.
            new_active_session()
            try:
                value_by_attr = {}
                for attr, postgresql_value in izip(attrs, record):
                    # The postgresql cursor returns things in its own encodings.
                    if postgresql_value is None:
                        record_value = None
                    elif attr.value_class == unicode:
                        record_value = postgresql_value.decode('utf-8')
                    elif attr.value_class == UUID:
                        record_value = UUID(postgresql_value)
                    elif attr.enum and not attr.is_set_like:
                        record_value = attr.enum[postgresql_value]
                    elif attr.value_class == datetime:
                        if -1 == postgresql_value:
                            record_value = None
                        else:
                            record_value = from_hsntime(postgresql_value)
                    elif attr.value_class == date:
                        if -1 == postgresql_value:
                            record_value = None
                        else:
                            record_value = from_hsndate(postgresql_value)
                    elif attr.value_class == time:
                        if -1 == postgresql_value:
                            record_value = None
                        else:
                            record_value = from_timestorage(postgresql_value)
                    elif attr.related_attribute and not attr.enum:
                        key_value = attr.entity_class.get_key_attribute().value_class(postgresql_value)
                        record_value = attr.related_attribute.entity_class(key_value)
                    else:
                        record_value = postgresql_value
                    value_by_attr[attr.name] = record_value
            except Exception, e:
                msg = 'SQL record for %s raised exception %s' % (
                    entity_class.__name__, e)
                stats.add_error(msg)
                unfixed_errors += 1
            else:
                unfixed_errors += _repair_cassandra_from_postgres_by_record(
                    entity_class,
                    value_by_attr,
                    scalars,
                    incrs,
                    stats,
                    overwrite=overwrite,
                    repair=repair,
                    verify=verify,
                    blank_equals_none=blank_equals_none,
                    attributes=attributes,
                    force=force,
                    dry_run=dry_run,
                    transaction_timestamp=transaction_timestamp)
    return unfixed_errors

def _repair_cassandra_from_postgres_by_record(
        entity_class,
        record,
        scalars,
        incrs,
        stats,
        overwrite=False,
        repair=False,
        verify=True,
        blank_equals_none=False,
        attributes=None,
        force=None,
        dry_run=False,
        transaction_timestamp=None):

    unfixed_errors = 0
    unfixed_errors += _repair_cassandra_from_postgres_by_column_family(
        entity_class,
        record,
        scalars,
        incrs,
        stats,
        is_counter=False,
        index=None,
        overwrite=overwrite,
        repair=repair,
        verify=verify,
        blank_equals_none=blank_equals_none,
        attributes=attributes,
        force=force,
        dry_run=dry_run,
        transaction_timestamp=None)

    if incrs:
        unfixed_errors += _repair_cassandra_from_postgres_by_column_family(
            entity_class,
            record,
            scalars,
            incrs,
            stats,
            is_counter=True,
            index=None,
            overwrite=overwrite,
            repair=repair,
            verify=verify,
            blank_equals_none=blank_equals_none,
            attributes=attributes,
            force=force,
            dry_run=dry_run,
            transaction_timestamp=transaction_timestamp)

    for index in entity_class.persistent_class.indexes.keys():
        unfixed_errors += _repair_cassandra_from_postgres_by_column_family(
            entity_class,
            record,
            scalars,
            incrs,
            stats,
            is_counter=False,
            index=index,
            overwrite=overwrite,
            repair=repair,
            verify=verify,
            blank_equals_none=blank_equals_none,
            attributes=attributes,
            force=force,
            dry_run=dry_run,
            transaction_timestamp=transaction_timestamp)

    return unfixed_errors

def _repair_cassandra_from_postgres_by_column_family(
        entity_class,
        record,
        scalars,
        incrs,
        stats,
        is_counter=False,
        index=None,
        overwrite=False,
        repair=False,
        verify=True,
        blank_equals_none=False,
        attributes=None,
        force=None,
        dry_run=False,
        transaction_timestamp=None):
    if is_counter:
        attrs = incrs
        column_family = entity_class.get_counter_column_family()
    else:
        attrs = scalars
        if index:
            column_family = entity_class.get_index_column_family(index)
        else:
            column_family = entity_class.get_primary_column_family()
    names = tuple(a.name for a in attrs)
    not_in_cassandra = False
    repairs = {}  # TODO: use this to make verify a one-liner in the log.
    primary_key = record[entity_class.key_attribute_name]
    fetcher = Fetcher(cf_name=column_family, fields=names)
    if verify or not overwrite:
        if index:
            has_all_keys = True
            for keyname in entity_class.persistent_class.indexes[index]:
                if attributes and keyname not in attributes:
                    return 0
                attr = entity_class.get_attribute(keyname)
                val = record[keyname]
                if val is None or val == '':
                    has_all_keys = False
                    break
                fetcher.add_column_value_relation(keyname, attr.cassandra_encode(val))
        else:
            attr = entity_class.get_key_attribute()
            fetcher.add_column_value_relation(entity_class.key_attribute_name, attr.cassandra_encode(primary_key))

        if index and not has_all_keys:
            return 0
        result = fetcher.fetch_first()
        if not result:
            not_in_cassandra = True
            if verify:
                stats.add_error(u'%s(%s) not in c(%s)' % (
                    entity_class.__name__, primary_key, column_family))
        else:
            for attr, val in izip(attrs, result):
                pval = record[attr.name]
                cval = attr.cassandra_decode(val)
                if pval != cval:
                    if not (blank_equals_none and pval in [None, ''] and cval in [None, '']):
                        repairs[attr.name] = attr.cassandra_encode(pval)
                        if verify:
                            stats.add_error(u'%s(%s).%s pg %s    c(%s) %s' % (
                                entity_class.__name__, primary_key, attr.name, pval, column_family, cval))
                elif verify:
                    if type(pval) != type(cval):
                        if not (blank_equals_none and pval in [None, ''] and cval in [None, '']):
                            stats.add_error(u'%s(%s).%s pg %s     c(%s) %s' % (
                                entity_class.__name__, primary_key, attr.name, type(pval), column_family, type(cval)))
    if not_in_cassandra or overwrite:
        if is_counter:
            new_repairs = {}
        else:
            new_repairs = {'hsn_deleted': False}
        for attr in attrs:
            attr_name = attr.name
            value = record[attr_name]
            attr = entity_class.get_attribute(attr_name)
            value = attr.cassandra_encode(value)
            new_repairs[attr_name] = value
        repairs = new_repairs
    if not dry_run and repair and repairs:
        if is_counter:
            # You can't make a counter column None, Cassandra behavior in this
            # case is undefined.
            for k, v in repairs.copy().iteritems():
                if v is None:
                    del repairs[k]
            if repairs:
                key = entity_class.get_key_attribute().cassandra_encode(primary_key)
                repairs[entity_class.key_attribute_name] = key
                batch(increments={
                    (column_family, (entity_class.key_attribute_name,),): [repairs]},
                     consistency_level='QUORUM', timestamp=transaction_timestamp)
        else:
            write_ok = True
            if index:
                key_tup = entity_class.persistent_class.indexes[index]
                for key_name in entity_class.persistent_class.indexes[index]:
                    attr = entity_class.get_attribute(key_name)
                    key_value = record[key_name]
                    if key_value is None or key_value == '':
                        write_ok = False
                        break
                    # In the case of overwrite we may have skipped checking
                    # for the missing key.
                    repairs[key_name] = attr.cassandra_encode(key_value)
            else:
                key_tup = (entity_class.key_attribute_name,)
                key_attribute = entity_class.get_key_attribute()
                record_value = record[key_attribute.name]
                encoded_value = key_attribute.cassandra_encode(record_value)
                repairs[entity_class.key_attribute_name] = encoded_value
            if write_ok:
                batch(cfs_data_pairs={
                    (column_family, key_tup): [repairs]},
                     consistency_level='QUORUM', timestamp=transaction_timestamp)
    if repairs and (dry_run or not repair):
        return 1
    else:
        return 0
        

def verify_postgresql_entities_in_cassandra(count=10,
                                            print_errors=True,
                                            report_none_vs_blank=False):
    errors = []
    entity_count = 0
    import elixir
    for entity in elixir.entities:
        if entity.saves_to_postgresql:
            num = verify_postgresql_entity_class_in_cassandra(
                entity, count, errors=errors, print_errors=False,
                report_none_vs_blank=report_none_vs_blank)
            entity_count += num
    if print_errors:
        if logger.isEnabledFor(INFO):
            if errors:
                logger.info('\n'.join(errors))
            logger.info('verify_postgresql_entities_in_cassandra read %s with %s',
                        pluralize(entity_count, 'entity'),
                        pluralize(errors, 'error'))
    return len(errors)

def verify_postgresql_entity_class_in_cassandra(entity_class,
                                                count=1,
                                                print_errors=True,
                                                report_none_vs_blank=False,
                                                errors=None):
    if errors is None:
        errors = []
    from sqlalchemy.sql.expression import func
    entity_count = 0
    for i in xrange(count):
        random_persistent = entity_class.persistent_class.query.order_by(func.random()).first()
        if random_persistent is None:
            break
        verify_postgresql_entity_in_cassandra(random_persistent,
                                              print_errors=False,
                                              report_none_vs_blank=report_none_vs_blank,
                                              errors=errors)
        entity_count += 1
    if print_errors:
        if logger.isEnabldFor(INFO):
            if errors:
                logger.info('\n'.join(errors))
            logger.info('verify_postgresql_entity_class_in_cassandra read %s with %s',
                        pluralize(entity_count, 'entity'),
                        pluralize(errors, 'error'))
    return entity_count

def verify_postgresql_entity_in_cassandra(persistent,
                                          print_errors=True,
                                          report_none_vs_blank=False,
                                          errors=None):
    if errors is None:
        errors = []
    from backend.schema import Incrementer, OneToOne

    def attr_in_table(attr):
        """True if this attribute holds its value in its entity's own table."""
        if attr.is_set_like:
            return False
        if isinstance(attr, Incrementer):
            return False
        if isinstance(attr, OneToOne):
            return False
        return True

    attrs = list(persistent.entity_class.attributes.values())
    scalars = [a for a in attrs if attr_in_table(a)]
    incrs = [a for a in attrs if isinstance(a, Incrementer)]

    _verify_postgresql_entity_in_cassandra(
            persistent, scalars,
            report_none_vs_blank=report_none_vs_blank,
            errors=errors)
    for index in persistent.indexes.keys():
        _verify_postgresql_entity_in_cassandra(
                persistent, scalars, index=index,
                report_none_vs_blank=report_none_vs_blank,
                errors=errors)
    if incrs:
        _verify_postgresql_entity_in_cassandra(
                persistent, incrs, counters=True,
                report_none_vs_blank=report_none_vs_blank,
                errors=errors)
    if print_errors:
        if logger.isEnabledFor(INFO):
            if errors:
                logger.info('\n'.join(errors))
            logger.info('verify_postgresql_entity_in_cassandra %s had %s',
                        persistent, pluralize(errors, 'error'))

def _verify_postgresql_entity_in_cassandra(persistent, attrs,
                                           index=None, counters=False,
                                           report_none_vs_blank=False,
                                           errors=None):
    entity_class = persistent.entity_class
    field_names = tuple(a.name for a in attrs)
    if counters:
        column_family = entity_class.get_counter_column_family()
    elif index:
        column_family = entity_class.get_index_column_family(index)
    else:
        column_family = entity_class.get_primary_column_family()
    fetcher = Fetcher(cf_name=column_family, fields=field_names)
    if index:
        has_all_keys = True
        for keyname in persistent.indexes[index]:
            attr = entity_class.get_attribute(keyname)
            val = getattr(persistent, keyname)
            if val is None or val == '':
                has_all_keys = False
                break
            fetcher.add_column_value_relation(keyname, attr.cassandra_encode(val))
    else:
        attr = entity_class.get_key_attribute()
        fetcher.add_column_value_relation(entity_class.key_attribute_name, attr.cassandra_encode(persistent.key))

    if index and not has_all_keys:
        return 0
    result = fetcher.fetch_first()
    if not result:
        errors.append('%s not found in Cassandra %s' % (
            persistent, column_family))
    else:
        for attr, val in izip(attrs, result):
            pval = getattr(persistent, attr.name)
            if attr.related_attribute and pval:
                pval = attr.value_class(pval.key)
            cval = attr.cassandra_decode(val)
            if pval != cval:
                if report_none_vs_blank or pval not in [None, ''] or cval not in [None, '']:
                    errors.append('%s.%s pg %s    c(%s) %s' % (
                        persistent, attr.name, pval, column_family, cval))
            if type(pval) != type(cval):
                if report_none_vs_blank or pval not in [None, ''] or cval not in [None, '']:
                    errors.append('%s.%s pg %s     c(%s) %s' % (
                        persistent, attr.name, type(pval), column_family, type(cval)))


SERIALIZER = {
    'int': lambda i: pack('>i', i)
}
def serialize_value(value, value_class_name):
    return SERIALIZER[value_class_name](value)

DESERIALIZER = {
    'int': lambda i: unpack('>i', i)
}
def deserialize_value(serialized_value, value_class_name):
    return DESERIALIZER[value_class_name](serialized_value)

QUERIES = {}

# A note for future migrations:
#Adding Columns with ALTER TABLE
#The ALTER TABLE command adds new columns to a column family. For example, to
# add a coupon_code column with the varchar validation type to the users
# column family:
#
#cqlsh:demodb> ALTER TABLE users ADD coupon_code varchar;
#This creates the column metadata and adds the column to the column family
#schema, but does not update any existing rows.
#
#Altering Column Metadata
#Using ALTER TABLE, you can change the type of a column after it is defined
#or added to a column family. For example, to change the coupon_code column to
#store coupon codes as integers instead of text, change the validation type as
#follows:
#
#cqlsh:demodb> ALTER TABLE users ALTER coupon_code TYPE int;
#Only newly inserted values, not existing coupon codes are validated against
#the new type.
#---
# note this is how you do an increment:
# 'UPDATE testfoo SET plays = plays+1 WHERE uuid=%s' % key
