
import logging
from uuid import UUID, uuid1

from backend.core import _class_repr, generate_id, get_active_session
from backend.exceptions import AllAttributesNotLoaded, \
                               AssignmentNotAllowed, \
                               AttributeNotLoaded, \
                               KeyNotLoaded
from backend.loader import load, Loader


logger = logging.getLogger("hsn.backend")


class Environment(object):
    """State of the backend system whether known, cached, unknown or local."""

    def get(self, entity_class, key):
        """Get entity from this Environment if it exists, else None."""
        raise NotImplementedError

    def get_peer(self, entity):
        """Get counterpart entity in this Environment if exists, else None."""
        if entity.environment is self:
            return entity
        return self.get(entity.entity_class, entity.key)

    def fetch_peer(self, entity):
        """Return the counterpart entity in this Environment if it exists.
        
        If not, create such a counterpart entity and return it.

        """
        if object.__getattribute__(entity, 'environment') == self:
            return entity
        assert(object.__getattribute__(entity, 'key') is not None)
        return self.fetch(object.__getattribute__(entity, 'entity_class'),
                          object.__getattribute__(entity, 'key'),
                          has_temporary_key=object.__getattribute__(
                            entity, 'has_temporary_key'))

    # should be called 'create_if_not_already_exist_in_which_case_get' but
    # that is too long. The emphasis here is on create, and 'find' is what we
    # do in the case of a duplicate.
    def fetch(self, entity_class, key=None, has_temporary_key=False):
        """
        Return the entity specified in this Environment if it exists. If not,
        create such an entity and return it.

        """
        raise NotImplementedError

    def fetch_environment(self):
        """
        Returns an Environment a Session may use as a loaded environment.

        Note that the environment returned may or may not be 'self'.

        """
        raise NotImplementedError

    @classmethod
    def on_load_class(cls):
        """
        Called when a sugared load method is passed this class as an argument.

        Return an environment a Session may use as a loaded environment.

        """
        raise NotImplementedError

    def set_attribute(self, entity, attribute, value):
        """
        Set the value of this entity's attribute in this Environment to be
        the given value.

        Returns local peer of value.

        """
        raise NotImplementedError

    def get_attribute(self, entity, attribute):
        """
        Get the value of this entity's attribute as it is known in this
        environment. Raise AttributeNotLoaded otherwise.

        """
        raise NotImplementedError

    def load(self):
        """
        A sugar-supporting request to load and add an Environment to the set
        of loaded environments in the current session.

        """
        raise NotImplementedError

    def save(self, session=None):
        """Queue this environment in the current session's save_now queue."""
        if session is None:
            session = get_active_session()
        session.save_now |= set([self])

    def save_later(self, session=None):
        """Queue this environment in the current session's save_later queue."""
        if session is None:
            session = get_active_session()
        session.save_later |= set([self])

    def exists(self, entity):
        raise NotImplementedError


class EntityMetaclass(type):
    # This is called every time an Entity is instantiated.
    def __call__(cls,
                 key=None,
                 environment=None,
                 has_temporary_key=False,
                 write_only=False,
                 *args, **kwargs):
        # If this Entity exists in the Environment, return it. Otherwise make
        # an Entity for this Environment.
        if environment is None:
            import backend
            environment = backend.get_active_session()
            # Try a fast get from the session.
            try:
                instances_by_key = environment.instances_by_class[cls.__name__]
            except KeyError:
                pass
            else:
                try:
                    return instances_by_key[key]
                except KeyError:
                    pass
            
        if key is None:
            # If no key is provided, generate a key and mark it temporary.
            assert has_temporary_key is None or has_temporary_key == False
            has_temporary_key = True
            # Create a temporary key
            from backend import generate_id
            value_class = cls.get_key_attribute().value_class
            if value_class != UUID:
                key = cls.get_key_attribute().value_class(generate_id())
            else:
                key = generate_id()
        else:
            if __debug__:
                value_class = cls.get_key_attribute().value_class
                if value_class == UUID:
                    error = type(key).__name__ not in ['UUID', 'FastUUID']
                else:
                    error = not isinstance(key, cls.get_key_attribute().value_class)
                if error:
                    logger.error('%s key is %s, of type %s -- expected %s to be type %s' % (
                cls, key, type(key), cls.get_key_attribute().name, value_class))
        return environment.fetch(cls, key, has_temporary_key=has_temporary_key,
                                 write_only=write_only, *args, **kwargs)


class Entity(object):
    """
    Schema defined object used to interface with Backend.

    An instance of an Entity is a reference to a Backend object of a
    particular entity class with a unique key in an Environment.

    An entity relies on its Environment to read and write data.

    It is best to think of an Entity as a handle.

    """
    __metaclass__ = EntityMetaclass
    # Attributes by attribute name. See method _setup_attributes.
    attributes = {}
    # All attributes, in order. (Presumed faster than iterating a dict.)
    attributes_order = []

    # Subclasses must specify the following:
    # Persistent declaring the Fields and SQLAlchemy storage object. (schema)
    persistent_class = None
    # Primary Key to use for referring to an Entity object.
    key_attribute_name = 'uuid'
    # Special consistency requirements for saving to Cassandra.
    consistency = None

    @property
    def is_entity(self):
        return True

    @property
    def is_persistent(self):
        return False

    def promote(self, attr_name, value):
        """Set the given value in session memory without creating a delta."""
        self.environment.promote(self, attr_name, value)

    # Called at system initialization. Class variables do not pickle, so these
    # need to be set up so that cross-python-instance picked environments will
    # still have attributes sets to access.
    @classmethod
    def _setup_attributes(cls):
        """Initialize Attribute class variables using schema declaration."""
        attributes = {}
        attributes_order = []
        autoloads = []
        fields = cls.persistent_class.fields()

        for field in fields:
            attribute = field.make_attribute(cls)# Attribute.from_field(field, cls)
            attributes[attribute.name] = attribute
            attributes_order.append(attribute)
            if attribute.autoload:
                autoloads.append(attribute)

        # Iterate through the class looking for non-elixir Field Fields.
        from backend.schema import OneToNPOneField, OneToNPManyField
        pcls = cls.persistent_class
        attribute_names = dir(pcls)
        for attribute_name in attribute_names:
            field = getattr(pcls, attribute_name)
            if isinstance(field, (OneToNPOneField, OneToNPManyField)):
                field.name = attribute_name
                attribute = field.make_attribute(cls)
                attributes[attribute.name] = attribute
                attributes_order.append(attribute)
                if attribute.autoload:
                    autoloads.append(attribute)

        # Store the mapping to this instance. (should be per-entity subclass.)
        cls.attributes = attributes
        cls.attributes_order = attributes_order
        cls.autoloads = autoloads
        cls.autoload_set = frozenset(autoloads)

        sql_primary_key_attribute = None
        for attribute in attributes_order:
            if attribute.is_sql_primary_key:
                if not sql_primary_key_attribute is None:
                    logger.error(u'%s has more than one SQL primary key.')
                    raise NotImplementedError
                sql_primary_key_attribute = attribute
        assert sql_primary_key_attribute is not None, u'%s has no SQL primary key attribute.'
        cls.sql_primary_key_attribute = sql_primary_key_attribute

# from query
#        # Setup the mapping in the Entities that refer to this object.
#        from backend.schema import _SymmetricQuery
#        from backend.core import QUERIES
#        cls.object_class = get_class(cls.object_class_name)
#        query = _SymmetricQuery(cls.attribute_name,
#                                 cls,
#                                 cls.object_class,
#                                 default=[])
#        cls.object_class.attributes[query.name] = query
#        cls.object_class.attributes_order.append(query)
#        assert query.autoload == False
#        QUERIES[query.query_id] = query
# end query

    def has_attribute(self, name):
        try:
            val = self.environment._memory_get_attribute(
                self, self.attributes[name], generate_incomplete_list=False)
            return True
        except AttributeNotLoaded:
            return False

    @classmethod
    def get_attribute(cls, name):
        try:
            return cls.attributes[name]
        except KeyError:
            return None

    @classmethod
    def get_key_attribute(cls):
        return cls.get_attribute(cls.key_attribute_name)

    # Note that Entity uses a singleton for each type:key_attribute_name
    # in each environment, so this only gets called once per
    # type:key_attribute_name per environment.
    def __init__(self,
                 environment=None,
                 key=None,
                 has_temporary_key=None,
                 write_only=False,
                 *args, **kwargs):
        object.__setattr__(self, 'environment', environment)
        # Primary key for this object in storage systems.
        object.__setattr__(self, 'key', key)
        object.__setattr__(self, 'has_temporary_key', has_temporary_key)
        # True if this object has been marked for deletion on next save.
        object.__setattr__(self, '_deleted', False)
        # True if this object was implied by the schema but not loaded.
        object.__setattr__(self, 'write_only', write_only)
        object.__setattr__(self, 'is_session_entity', False)
        object.__setattr__(self, 'load_failed', False)
        object.__setattr__(self, 'failed_loaders', [])
        object.__setattr__(self, '__reprmemo__', "%s(u'%s')" % (_class_repr(self), key))
        # Note: This assumes that all type names are unique, so nothing like
        # model.obj.Foo and model.person.Foo.
        object.__setattr__(self, '__hashmemo__', str('__entity__%s_%s' % (
            self.entity_class.__name__, key)).__hash__())

        super(Entity, self).__init__()

    @classmethod
    def create(cls, key=None):
        """Create a new Backend object."""
        key_attribute = cls.get_key_attribute()
        if key is None:
            try:
                if key_attribute.value_class != UUID:
                    # Only UUID is generatable.
                    raise NotImplementedError
            # This is detecting a vexatious case when missed.
            except AttributeError:
                logger.error('key_attribute.value_class error.  Could %s be missing from model.imports?' % cls.__name__)
                raise
            if 'TimeUUIDField' == type(key_attribute.field_instance).__name__:
                key = uuid1()
            else:
                key = generate_id()
        elif not isinstance(key, key_attribute.value_class):
            raise ValueError, "Created %s with key attribute '%s' %s of type %s, must be type %s" % (
                cls.__name__, key_attribute.name, key, type(key), key_attribute.value_class)

        from backend import get_active_session
        entity = get_active_session().create(cls, key)
        # If the key is not this object's primary key, generate a uuid for
        # the primary key.
        if key_attribute != cls.sql_primary_key_attribute and cls.sql_primary_key_attribute.value_class == UUID:
            setattr(entity, cls.sql_primary_key_attribute.name, generate_id())
        return entity

    @property
    def created(self):
        return self in self.environment.newly_created

    def __repr__(self):
        return object.__getattribute__(self, '__reprmemo__')

    def __eq__(self, other):
        #Have to check if other has a __hash__ function.
        # if it doesn't, that implies it's not an entity or a persistent
        return other.__hash__ and (object.__getattribute__(self, '__hashmemo__') == other.__hash__())

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return object.__getattribute__(self, '__hashmemo__')


    def _substitute(self, other):
        """
        Cause this write-only Entity to act exactly like the given 'other'
        Entity.

        """
        # NOTE -- WARNING -- This changes the hash of an Entity. Any set
        # or dict containing an Entity so altered will not recognize the new
        # hash.
        # Example:
        # entity = FooEntity(1)
        # s = set([entity])
        # entity._substitute(FooEntity(2))
        # assert entity not in s
        logger.debug("write only substitute %s replaces %s" % (other, self))
        assert self.entity_class == other.entity_class
        assert self.environment == other.environment
        assert self.write_only
        assert not self._deleted
        assert not other.write_only
        assert not other.has_temporary_key
        assert not other._deleted
        self.key = other.key
        self.has_temporary_key = False
        self._deleted = False
        self._write_only = False
        logger.debug("write only substitute finished. %s became %s" % (self, other))

    def delete(self):
        """Marks this object for deletion at next save."""
        self.environment.delete(self)

    @property
    def exists(self):
        """Confirm the existence of this object in Backend."""
        return self.environment.exists(self)

    def save(self, arg):
        """
        Queue this Entity to be saved to the 'arg' Environment(s)
        at next Session save.

        'arg' may be an Environment or a list of Environments.
        
        """
        self._queue_save(arg, 'save_now')

    def save_later(self, arg):
        """
        Queue this Entity to async-save to the 'arg' Envinorment(s)
        at next Session save.

        'arg' may be an Environment or a list of Environments.

        """
        self._queue_save(arg, 'save_later')

    def _queue_save(self, arg, queue_name):
        logger.debug("queue save %s %s %s" % (self, arg, queue_name))
        if not isinstance(arg, list):
            arg = [arg]
        from backend.environments.session import Session
        if not isinstance(self.environment, Session):
            raise IllegalCommand(u'Can only call save on a Session Entity.')
        session = self.environment
        # Check for non-instantiated EnvironmentCaches (classes), and
        # instantiate them.
        queue_entries = []
        for env in arg:
            if isinstance(env, type):
                # See if any of these environments will save this entity.
                found = False
                logger.debug("checking %s loaded environments" % len(session.loaded_environments))
                for loaded in session.loaded_environments:
                    logger.debug("checking %s" % loaded)
                    if isinstance(loaded, env):
                        # They are calling a write-back.
                        # Check if this instance of the environment is paired
                        # with this entity.
                        from backend import MainPersistent
                        if isinstance(loaded, MainPersistent):
                            logger.debug("found main persistent")
                            found=True
                            queue_entries.append(loaded)
                        elif ((loaded.entity_class == self.entity_class) and
                              (loaded.entity_key == getattr(self,
                                                            self.key_attribute_name))):
                            logger.debug("found %s" % loaded)
                            found=True
                            queue_entries.append(loaded)
                if not found:
                    # They are calling a write-to.
                    logger.debug("Cache save is a write-to %s" % env)
                    queue_entries.append(env.from_peer(self))
            else:
                queue_entries.append(arg)
        setattr(session,
                queue_name,
                getattr(session, queue_name) | set(queue_entries))
        logger.debug("save now: %s" % session.save_now)
        logger.debug("save later: %s" % session.save_later)

    # Note that this may only be called on a Session's entity. It is
    # Sugar indicating that the session should load the Environment
    # indicated using an association with this object. Ex: obj.load(FooCache)
    # Adds the FooCache for the object 'obj' to the Session's loaded
    # environments.
    def load(self, sources, pipeline=None, refresh=True, consistency=None,
             suppress_misses=False):
        """Syntactic sugar for session.load

        sources -- Caches to load (each whose key is this Entity's key)
        pipeline -- A redis pipeline to use for our first redis request(s).
        refresh -- If True, on a miss these Caches refresh from SQL to Redis.
        consistency -- Forced minimum Cassandra consistency.  None for default.
        suppress_misses -- If True, missing and unitializable Loaders will not
           raise NullEnvironmentError, but will cause an Entity to exist in the
           session.

        Obj(u'foo').load(FooCache)
        is equivalent to
        get_active_session().load(FooCache(u'foo'))

        Accepts a single cache or a list of caches.

        A tuple may be passed in as well (FooCache, Foo(u'foo')) so that caches
        for other objects may be added to the sugared call.

        Obj(u'foo').load([FooCache, (FooCache, Foo(u'foo2'),)])

        """
        # Groom the inputs, if a non-list was given, listatize it.
        list_types = (set, list, frozenset,)
        if not isinstance(sources, list_types):
            sources = [sources]
        
        if not sources:
            from backend import MainPersistent
            sources = [MainPersistent]

        if pipeline is not None:
            if not type(pipeline).__name__.endswith('ipeline'):
                raise ValueError('pipeline was %s, needs to be None or a Redis pipeline' % pipeline)
            if not isinstance(refresh, bool):
                raise ValueError('refresh was %s, needs to be bool' % pipeline)
        assert not self.has_temporary_key
        instances = []
        loaders = []
        for s in sources:
            instance = None
            if issubclass(s, Loader):
                loaders.append(s(self.key))
            elif isinstance(s, type):
                instance = s.from_peer(self)
            elif isinstance(s, tuple):
                assert(len(s) == 2)
                instance = s[0].from_peer(s[1])
            else:
                instance = s
            if instance:
                instances.append(instance)

        if loaders:
            load(loaders,
                 consistency=consistency,
                 suppress_misses=suppress_misses)
        if instances:
            return self.environment.load(instances,
                                         pipeline=pipeline,
                                         refresh=refresh,
                                         suppress_misses=suppress_misses)

    def __setattr__(self, name, value):
        attribute = self.get_attribute(name)        
        if attribute:
            if attribute.entity_class.persistent_class.indexes and not self.created and type(attribute).__name__ != 'Incrementer':
                # Examine the indexes that this entity participates in and confirm
                # we have sufficient information to perform a save.
                for index, index_attrnames in attribute.entity_class.persistent_class.indexes.iteritems():
                    # If any index has keys that are not loaded, we raise.
                    # If it has empty keys, it is incomplete, if it has
                    # all the other keys present it is complete.
                    has_missing = False
                    has_none = False
                    for attrname in index_attrnames:
                        if attrname == attribute.name:
                            continue
                        try:
                            existing_other_value = self._attribute_value(attribute.entity_class.get_attribute(attrname))
                        except AttributeNotLoaded:
                            has_missing = attrname
                        else:
                            if existing_other_value is None or existing_other_value == '':
                                has_none = True
                                break
                    # If there is an empty key in there, it won't write,
                    if has_none:
                        pass
                    # If all the keys have values but one was missing, raise.
                    elif has_missing:
                        raise KeyNotLoaded('Due to index %s you must load or promote a value for %s.%s before assigning a new value to %s.' % (index, self, has_missing, attribute.name))
            if attribute.indexes and not self.created:
                # If this is a key in an index, we need to check some conditions.
# if you assign any index-having entity a non-increment attribute you must
# have all of the composite keys loaded.  (or at least one None key for each
# index.)
# if you assign a key attribute a value and...
# * entity is created during this session -- No error is raised.
# * entity was created in a previous session, and key is...
#    + not loaded
#        x raise a KeyNotLoaded error.  Either load the keys from the primary
#          table or (if you want to hack this promote empty values for the
#          other keys without loading.)
#    + loaded and empty, and other keys in any index this key is in are...
#        x not loaded -- raise a KeyNotLoaded error.  Either load the keys from
#                        the primary table or (if you want to hack this)
#                        promote empty values for the other keys without loading.
#        x loaded and at least one is empty  -- no error is raised, no table is written on save.
#           (on assignment, check each index for empty keys, if all indexes
#            have at least one empty key, you are done.)
#        x loaded and at least one index has all the other values present...
#            - if all values for the object are not known (loaded or assigned in this session) -- raise AllAttributesNotLoaded error.
#            - if all values for the object are known (loaded or assigned in this session) -- no error is raised, on save write to index tables that this assigned key is a key in.
#    + loaded and not empty and...
#        x there is an index with unknown keys... raise a KeyNotLoaded error (either load or promote to remedy)
#        x there is an index with all the other keys known... raise an AssignmentNotAllowed error, as this would cause an index table to be wrong (or cause an error when we try to write to it.)
#        x there are no indexes with all the other keys known... save to primary table, no save to index tables will occur.
# note: race conditions -- assume an entity with index keyA,keyB.  If keyA is
# assigned in one process and keyB assigned in another, it is possible that
# the index table will never be written to.  Caveat Developer.

                # Is this a key the value of the key already known?  If not, raise.
                try:
                    existing_value = object.__getattribute__(self, attribute.name)
                except:
                    try:
                        existing_value = object.__getattribute__(self, 'environment').get_attribute(self, attribute, create_write_only=False)
                    except AttributeNotLoaded:
                        raise KeyNotLoaded('Load or promote a value for %s.%s before assigning a new value.' % (self, attribute.name))

                # Examine the indexes that this key participates in and confirm
                # we have sufficient information to perform a save.
                checked_all_attributes_loaded = False
                for index in attribute.indexes:
                    index_attrnames = attribute.entity_class.persistent_class.indexes[index]
                    # If an index has keys that are not loaded, we raise.
                    # If it has empty keys, it is incomplete, if it has
                    # all the other keys present it is complete.
                    has_missing = False
                    has_none = False
                    for attrname in index_attrnames:
                        if attrname == attribute.name:
                            continue
                        try:
                            existing_other_value = self._attribute_value(attribute.entity_class.get_attribute(attrname))
                        except AttributeNotLoaded:
                            has_missing = attrname
                        else:
                            if existing_other_value is None or existing_other_value == '':
                                has_none = True
                                break
                    # If there is an empty key in there, it won't write,
                    if has_none:
                        pass
                    # If all the keys have values but one was missing, raise.
                    elif has_missing:
                        raise KeyNotLoaded('Due to index %s you must load or promote a value for %s.%s before assigning a new value to %s.' % (index, self, has_missing, attribute.name))
                    # It would seem we have all the keys to perform a write.
                    else:
                        # If the existing value is not None, this would result
                        # in writing to the index table twice, so raise.
                        if not (existing_value is None or existing_value == ''):
                            raise AssignmentNotAllowed('%s.%s already has a value of %s and may not be reassigned.' % (self, attribute.name, existing_value))
                        # If the existing value is None, confirm we have 
                        # enough data to write to the index table.
                        else:
                            if not checked_all_attributes_loaded:
                                if not self._all_attributes_loaded():
                                    raise AllAttributesNotLoaded("%s index '%s' can not save because not all attribute values are loaded" % (self, index))
                                else:
                                    checked_all_attributes_loaded = True
#            #if attribute.indexes and not self.created:
#            # ...(from top)
#            else:
#                # Even if this attribute is not a key, if the entity_class has
#                # indexes then all keys must be known.
#                if not self.created and attribute.entity_class.indexes
#                pass
            peer = object.__getattribute__(self, 'environment').set_attribute(self, attribute, value)
            try:
                if object.__getattribute__(self, 'is_session_entity') and not object.__getattribute__(self, 'write_only'):
                #if not attribute.related_attribute:
                    object.__setattr__(self, name, peer)
            except:
                pass
        # If there is no field with this attribute name pass on to baseclass.
        else:
            object.__setattr__(self, name, value)

    def __getattribute__(self, name):
        """Get the value for a field name from this entity's environment ."""
        # First check if the value is on the object already, if so, use that.
        try:
            return object.__getattribute__(self, name)
        except:
            pass
        # Get the Attribute sought, note it is per class, not per instance.
        try:
            attribute = type(self).attributes[name]
        except KeyError:
            # If there is no field with this attribute name pass on to
            # baseclass.
            return object.__getattribute__(self, name)
        # Using the Attribute object for this Entity type, get the value.
        a = object.__getattribute__(self, 'environment').get_attribute(self, attribute)
        # We 'cache' the results as a dynamic attribute on the Entity.
        if object.__getattribute__(self, 'is_session_entity') and not self.write_only:
            object.__setattr__(self, name, a)
        return a

    def increment(self, attrname, value):
        self.environment.increment(self,
                                   self.entity_class.attributes[attrname],
                                   value)

    def _all_attributes_loaded(self):
        """Returns True if we have values for all of this entity's attributes.

        Increment values are ignored (as they go in a separate table.)

        Used to confirm that enough information is present to do a save to
        an index table.

        """
        try:
            for attribute in self.entity_class.attributes.values():
                if type(attribute).__name__ not in ('Incrementer', 'OneToMany', 'OneToOne'):
                    self._attribute_value(attribute)
        except AttributeNotLoaded:
            return False
        return True

    def _attribute_value(self, attribute):
        """get value if we have one, else raise AttributeNotLoaded."""
        try:
            return object.__getattribute__(self, attribute.name)
        except:
            return object.__getattribute__(self, 'environment').get_attribute(self, attribute, create_write_only=False)

    @classmethod
    def get_primary_column_family(cls):
        return 'b_%s' % cls.__name__.lower()

    @classmethod
    def get_counter_column_family(cls):
        return 'b_%s__counts' % cls.__name__.lower()

    @classmethod
    def get_index_column_family(cls, index):
        return 'b_%s_%s' % (cls.__name__.lower(), index.lower())
