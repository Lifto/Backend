
from datetime import datetime, timedelta
from itertools import izip
import logging
import sys

from django.conf import settings
from celery.decorators import task
from sqlalchemy.orm.collections import InstrumentedList

from util.admin import send_admin_error_email_no_req, send_admin_warn_email
from util.cache import get_redis
from util.helpers import ordinal, pluralize
from util.when import stringify, took
from backend.async import (process_now_queue, process_later_queue,
    _pop_later_queue, _process_async_tasks, _get_timeout)
from backend.core import generate_id
from backend.datastructures import MemorySet, IncompleteSet
from backend.environments.cache import Cache
from backend.environments.cassandra import Cassandra
from backend.environments.deltas import AppendManyToMany, \
                                        AppendOneToMany, \
                                        CreateEntity, \
                                        DeleteEntity, \
                                        DiscardManyToMany, \
                                        DiscardOneToMany, \
                                        IncrementAttribute, \
                                        SetAttribute
from backend.environments.memory import _EnvironmentMemory, SetHandle
from backend.environments.persistent import EnvironmentPersistent, \
                                            ExistsPersistent, \
                                            MainPersistent, \
                                            SearchDetector
from backend.exceptions import AttributeNotLoaded, \
                               DuplicateKeyError, \
                               EntityNotFoundError, \
                               NullEnvironmentError, \
                               RedisLoadError, \
                               TooManyDeltas


logger = logging.getLogger("hsn.backend")
nag_logger = logging.getLogger("hsn.backend.attribute_type_nagger")
MAX_DELTAS = int(settings.BACKEND_MAX_DELTAS)


class Session(_EnvironmentMemory):
    """An Environment that can load from multiple source Environments and
    orchestrate complex sync/async update behavior.

    """

    def __init__(self, *args, **kwargs):
        # List of all changes made to Entities in the Session not yet saved.
        self.deltas = []
        # List of environments from which to promote missing session data.
        self.loaded_environments = []
        # Set of incremented attributes. [class name][key][attribute name].
        self.increments = {}
        # Set of environments to apply deltas to at next save.
        self.save_now = set()
        self.save_later = set()
        # Caches to be refreshed async at next save.
        self.refresh_caches = set()
        # True if the session has been reset. (Prevents erroneous use.)
        self.session_closed = False
        # A list of newly created entities. (Created since last reset.)
        self.newly_created = set()
        # A set of loaded caches, to prevent redundant loading.
        self.caches = set()
        # A set of loaded loaders, to prevent redundant loading.
        # (see Loader.get_session_hash)
        self.loaders = {}
        # Similar to values, a set of entity-attribute-write_onlies used to
        # when substituting loaded (memory)values over write-onlies.
        # TODO: Should this just be in memory?
        self._write_only_connections = {}
        self._pipeline = None
        self.uuid = generate_id()
        super(Session, self).__init__(*args, **kwargs)

    def fetch(self,
              entity_class,
              key,
              has_temporary_key=False,
              write_only=False,
              *args, **kwargs):
        # Check memory for this entity.
        no_exception = True
        try:
            instances_by_key = self.instances_by_class[entity_class.__name__]
        except KeyError:
            no_exception = False
        if no_exception:
            try:
                instance = instances_by_key[key]
            except KeyError:
                no_exception = False
        if no_exception:
            return instance

        # Create the entity in memory if it was not found.
        entity = super(Session, self).fetch(entity_class,
                                                       key,
                                                       has_temporary_key=has_temporary_key,
                                                       write_only=write_only)
        entity.is_session_entity = True # KLUDGE to do dynamic attrs,
        # if this works, find a better way. 
        return entity

    def create(self,
               entity_class,
               key,
               has_temporary_key=False,
               write_only=False,
               *args, **kwargs):
        assert not self.session_closed
        # Check memory for this entity.
        existing_entity = super(Session, self).get(entity_class, key)
        if existing_entity is not None:
            raise DuplicateKeyError(
"Attempting to create an object with a key that already exists (%s, %s)" % (
    entity_class, key))

        # Create the entity in memory.
        entity = super(Session, self).fetch(entity_class,
                                            key,
                                            has_temporary_key=has_temporary_key,
                                            write_only=write_only)
        # Put a create delta into our Deltas because we are indicating creation.
        # Fetches don't need deltas to work. A Create is specific.
        self.add_delta(CreateEntity(entity))
        self.newly_created.add(entity)
        # Set up the defaults on this newly created object.
        for attribute in entity_class.attributes_order:
            if attribute.name not in [entity_class.key_attribute_name, 'id']:
                default = attribute.default
                if isinstance(default, list):
                    default = [i for i in attribute.default]
                if type(attribute).__name__ == 'Incrementer':
                    # self.increment won't set the value on the entity unless
                    # the value was already there.  As we know this did not
                    # exist until just now we'll set the entity's value as 0
                    # and then increment will increment it and create the
                    # delta and the value will not raise an AttributeNotLoaded
                    # if it is accessed in this session.
                    if attribute.default is None:
                        object.__setattr__(entity, attribute.name, None)
                    else:
                        object.__setattr__(entity, attribute.name, 0)
                        self.increment(entity, attribute, default)
                else:
                    setattr(entity, attribute.name, default)
        return entity

    def delete(self, entity):
        """Add a delete delta to this environment and effect memory."""
        assert not self.session_closed
        logger.debug("__ses__ delete %s", entity)
        super(Session, self).delete(entity)
        self.add_delta(DeleteEntity(entity))

    def exists(self, entity):
        # Rules of existence
        # * If you are newly created, you exist.
        if entity in self.newly_created:
            return True
        # * If you exist in a cache we loaded, you exist.
        # Note: patch-on-load Caches won't pass this test, and will fall back
        # to the subsequent checks.
        for environment in self.loaded_environments:
            got = environment.get_peer(entity)
            if got is not None:
                return True
        # * If you exist in the ExistenceCache, you exist. (Obj only.)
        # * We also include reserved words in the in existence cache,
        #   we need to account for that.
        # TODO: Make this a plug-in or something, I don't like the direct
        # reference to something in model.
        # TODO: we could include the ExistenceCache as a default loaded
        # environment.
        from model.user import User
        if isinstance(entity, User):
            from logic.users.existence import ExistenceCache
            existence_data = ExistenceCache.get_existence_data(entity.uuid)
            if existence_data is None:
                return False
            else:
                return True
        # * If you exist in MainPersistent, you exist.
        if entity.persistent_class.saves_to_postgresql:
            peer = MainPersistent().get_peer(entity)
            if peer is not None:
                return True

        # * If you exist in Cassandra, you exist.
        if entity.persistent_class.saves_to_cassandra:
            if Cassandra.exists(entity.entity_class, entity.key):
                return True

        # * Otherwise you don't exist.
        return False

    def get_attribute(self, entity, attribute, create_write_only=True):
        if not object.__getattribute__(entity, 'environment') == self:
            entity = self.fetch_peer(entity)

        # See if we have it in memory.
        try:
            val = self._memory_get_value(entity,
                                         attribute,
                                         generate_incomplete_list=False)
            if attribute.is_set_like:
                # See if we need to promote or merge a newly loaded value.
                # If the value already exists, and if there is a more recently
                # loaded value (no way to know) merge/replace the loaded value
                # with that one. (it's really just a fancy sort of promotion.)
                check_these = set(self.loaded_environments) - val._modifiers
                for e in check_these:
                    e_val = None
                    try:
                        from backend.environments.cache import Cache
                        if isinstance(e, Cache):
                            e_val = e._memory_get_value(entity,
                                                        attribute,
                                                        generate_incomplete_list=False)
                        else:
                            e_val = e.get_attribute(entity, attribute)
                    except (AttributeNotLoaded, EntityNotFoundError):
                        pass
                    if e_val is not None:
                        # What do we do here? We need to merge these...
                        # but sometimes merging makes a whole new dang thing!
                        # (like a MemorySet where once was an IncompleteSet)
                        if isinstance(e_val, SetHandle):
                            e_val = e_val._get_value()
                        if isinstance(val, IncompleteSet) and not isinstance(e_val, IncompleteSet):
                            # In this case we are making a new thing and merging.
                            new_thing = MemorySet()
                            for v in e_val:
                                if attribute.enum:
                                    new_thing.add(v)
                                else:
                                    new_thing.add(self.fetch_peer(v))
                            for v in val._contains:
                                new_thing.add(v)
                            for v in val._does_not_contain:
                                new_thing.discard(v)
                            new_thing._modifiers = set(list(val._modifiers))
                            new_thing._modifiers.add(e)
                            # Now we need to set this in memory...
                            self._memory_set_value(entity, attribute, new_thing)
                            val = new_thing
                        else:
                            # In this case just take whatever's in the other thing and merge in.
                            # But for now, just punt, or else we introduce other issues that we aren't
                            # ready yet. (like what if the two caches hold different data.)
                            pass
                    val._modifiers.add(e) # To prevent checking again.
#                logger.debug('__ses__ returning SetHandle from (%s, %s)' % (entity, attribute.name))
#                logger.debug("__ses__ the walue of which is: %s" % self._memory_get_value(entity, attribute))
                return SetHandle(self.get_peer(entity), attribute)
            else:
#                logger.debug("__ses__ returning %s" % val)
                if val is not None and attribute.value_class and issubclass(attribute.value_class, int) and not issubclass(attribute.value_class, bool):
                    return val + self._get_increment(entity, attribute)
                else:
                    return val
        except AttributeNotLoaded:
            pass
        
        found_something = False
        found_good_enough = False
        best_found = None
        def found_is_good_enough(found):
            if attribute.is_set_like:
                if isinstance(found, SetHandle):
                    return not isinstance(found._get_value(), IncompleteSet)
            return True

        for environment in self.loaded_environments:
            if not found_good_enough:
                try:
                    from backend.environments.cache import Cache
                    if isinstance(environment, Cache) and attribute.is_set_like:
                        # Peek at the value, skip if empty.
                        # (this raises AttributeNotLoaded if attribute not loaded.)
                        if entity is not None:
                            environment._memory_get_value(entity, attribute, generate_incomplete_list=False)
                    best_found = environment.get_attribute(entity, attribute)
                    if attribute.is_set_like:
                        assert isinstance(best_found, (SetHandle, InstrumentedList))
                    found_something = True
                    if attribute.enum and not attribute.is_set_like:
                        assert best_found is not None
                    if found_is_good_enough(best_found):
                        found_good_enough = True
                except AttributeNotLoaded:
                    pass

        if found_something:
            # Promote the value to the Session's _EnvironmentMemory.
            # Do not allow reverse relationships to be created.
            # (If they are needed they will promote when requested as well.)
            set_this = best_found
            if set_this is not None and attribute.value_class and issubclass(attribute.value_class, int) and not issubclass(attribute.value_class, bool):
                set_this += self._get_increment(entity, attribute)
            return_this = set_this
            if isinstance(best_found, SetHandle):
                assert attribute.is_set_like
                # If what we found was a list handle...
                # * We know we don't have it, or we wouldn't have looked.
                #   so first, make a local copy and set that in memory.
                set_this = best_found._make_local_copy_of_value(self)
                # And get a localized version of the handle.
                return_this = SetHandle(entity, attribute)
            else:
                if attribute.related_attribute:
                    if attribute.is_set_like:
                        assert isinstance(best_found, InstrumentedList)
                        new_best_found = MemorySet()
                        for v in best_found:
                            if attribute.enum:
                                new_best_found.add(attribute.enum[v.number])
                            else:
                                p = self.fetch_peer(v)
                                new_best_found.add(p)
                        set_this = new_best_found
                        return_this = SetHandle(entity, attribute)
                    elif best_found is not None:
                        set_this = self.fetch_peer(best_found)
                        return_this = set_this
            self._memory_set_value(entity, attribute, set_this)
            return return_this

        logger.debug("__ses__ not found in loaded environments.")
        # At this point an attribute is nowhere to be found.
        # (question... can write-onlies be later merged if loaded/discovered?
        # yes... but TODO. Think of a test. otherwise just don't do it.)

        # Get a write-only holder for this value if it is an Entity.
        # NOTE: Should a 'write only' be an entity with a writeonly flag?
        # well. .it's similar. it can also be a Set.
        if create_write_only and attribute.related_attribute:
            write_only = self.make_write_only_for(entity, attribute)
            if isinstance(write_only, SetHandle):
                # There are no reverse values to set up, and it is already in
                # memory.
                pass
            else:
                from backend.environments.core import Entity
                assert isinstance(write_only, Entity)
                # This method sets up reverse values.
                self._memory_set_attribute(entity, attribute, write_only)
            return write_only
        raise AttributeNotLoaded("Attribute Not Loaded: (%s, %s)" % (entity, attribute.name))

    def set_attribute(self, entity, attribute, value):
        # You shouldn't be setting anything outside on the session, at least
        # not now. So, while the other environments 'convert' value to a local
        # peer, it just so happens that Session doesn't do that. (well, it
        # could, but because we haven't built it to do that, it catches the
        # assumption here.)
# Took these checks out in hopes that it speeds things up.---
#        assert not self.session_closed
#        assert(not isinstance(value, Persistent))
#        assert(not isinstance(value, InstrumentedList))
#        if isinstance(value, Entity):
#            assert(value.environment == self)
#        if isinstance(value, list):
#            for v in value:
#                assert(not isinstance(v, Persistent))
#                if isinstance(v, Entity):
#                    assert(v.environment == self)
# end checks ---
        # Set the attribute on the Memory.
        type_name = type(attribute).__name__
        if not isinstance(entity.key, entity.get_key_attribute().value_class):
            raise ValueError, "Detected %s with key attribute '%s' of type %s, must be %s" % (
                entity.entity_class.__name__, entity.key_attribute_name, type(entity.key), entity.get_key_attribute().value_class)
        if type_name == 'Incrementer':
            msg = "Can't assign value to Incrementer %s.%s, must increment."
            raise TypeError(msg % (entity.entity_class.__name__, attribute.name))
        elif type_name == 'OneToOne' and value:
            # Because an assign on a OneToOne is an assign to the side of the
            # relationship that does not hold the key, let's make a delta
            # for an assign to the key-having side of the relationship.
            entity, value = value, entity  # swap entity and value
            attribute = attribute.related_attribute
        if value is not None and not attribute.is_set_like and not attribute.related_attribute:
            if not isinstance(value, attribute.value_class):
                #raise ValueError, '%s.%s was assigned %s(%s), it must be type %s' % (entity, attribute.name, type(value), value, attribute.value_class)
                nag_logger.error('%s.%s was assigned %s(%s), it must be type %s, attempting conversion' % (entity, attribute.name, type(value), value, attribute.value_class))
                import traceback
                for line in traceback.format_stack():
                    nag_logger.info(line)
                value = attribute.value_class(value)
        super(Session, self).set_attribute(entity, attribute, value)
        # Read the value we stored in Memory back.
        new_value = super(Session, self).get_attribute(entity, attribute)
        # Create and hold on to a Delta setting this value.
        # Note: new_value local peers for entity and entity-list values.
        if value is None:
            assert new_value is None
        else:
            assert new_value is not None
        assert not isinstance(new_value, (list, MemorySet, IncompleteSet, InstrumentedList))
        if isinstance(new_value, SetHandle):
            inner_value = new_value._get_value()
            assert isinstance(inner_value, MemorySet)
            new_value = [v for v in inner_value]
            assert len(value) == len(new_value), "Expected %s, got %s" % (value, new_value)
        self.add_delta(SetAttribute(entity, attribute, new_value))
        return new_value

    def increment(self, entity, attribute, value, create_delta=True):
        """Increment an integer value by the given amount."""
        if type(attribute).__name__ != 'Incrementer':
            msg = "Can't increment attribute of type %s, must be Incrementer."
            raise TypeError(msg % type(attribute).__name__)

        # If the value already resides on the entity, increment it there,
        # otherwise add to the increment set for local access.
        try:
            current_val = getattr(entity, attribute.name)
            if current_val is None:
                object.__setattr__(entity, attribute.name, value)
            else:
                object.__setattr__(entity, attribute.name, current_val + value)
        except AttributeNotLoaded:
            # The value was not set on the entity.  We can't set it on the
            # entity because the value may not be known, so it should raise
            # an AttributeNotLoaded if we attempt to access it even after
            # incrementing it.
            try:
                by_key = self.increments[entity.entity_class.__name__]
            except KeyError:
                by_key = {}
                self.increments[entity.entity_class.__name__] = by_key
            try:
                by_attribute = by_key[entity.key]
            except KeyError:
                by_attribute = {}
                by_key[entity.key] = by_attribute
            try:
                existing_increment = by_attribute[attribute.name]
            except KeyError:
                existing_increment = 0
            by_attribute[attribute.name] = existing_increment + value

        if create_delta:
            # Add an increment delta.
            d = IncrementAttribute(entity, attribute, value)
            self.add_delta(d)

    def promote_increment(self, entity, attribute, value):
        """Promote to an incrementer, accounting for pending incremens."""
        # If the value already resides on the entity...
        try:
            current_val = getattr(entity, attribute.name)
            #... and we are promoting a None, do nothing as we assume we
            # have already loaded (and possibly incremented) the value.
            if value is None:
                return
            #... otherwise, increment the value on the entity and we are done.
            object.__setattr__(entity, attribute.name, current_val + value)
            return
        except AttributeNotLoaded:
            # The value was not set on the entity.  Let's see if we are keeping
            # local increment that was added to session.increments, but was
            # not set on the Entity.  Note that We can't set a local-increment
            # on the entity when the existing value is not known, as it must
            # raise an AttributeNotLoaded if we attempt to access it even after
            # incrementing it.  But in this case we are going to set that
            # base value.  But first, find the pending increments.
            try:
                by_key = self.increments[entity.entity_class.__name__]
            except KeyError:
                by_key = {}
                self.increments[entity.entity_class.__name__] = by_key
            try:
                by_attribute = by_key[entity.key]
            except KeyError:
                by_attribute = {}
                by_key[entity.key] = by_attribute
            existing_increment = by_attribute.get(attribute.name)

            if value is None:
                new_value = existing_increment
            elif existing_increment is None:
                new_value = value
            else:
                new_value = existing_increment + value
            object.__setattr__(entity, attribute.name, new_value)

    def _get_increment(self, entity, attribute):
        """Return pending increment value, if any.

        only call this an an attribute with an 'int' value_class.
        
        """
        if __debug__:
            assert issubclass(attribute.value_class, int)
        try:
            entity_class = object.__getattribute__(entity, 'entity_class')
            by_key = self.increments[entity_class.__name__]
        except KeyError:
            return 0
        try:
            by_attribute = by_key[entity.key]
        except KeyError:
            return 0
        try:
            return by_attribute[attribute.name]
        except KeyError:
            return 0

    # A write only only exists in a Session. It is a delta concept.
    # An environment would just create the list or raise an error
    # The very thought of a write-only list is Session oriented, no other
    # sort of environment would have a reason to support such a concept.
    # (sql doesn't, cache makes no sense, neither cassandra.)
    # so, when a write only list is appiled it means "do appends" or
    # "do removes" (otherwise it is a value and not a write-only)
    def make_write_only_for(self, entity, attribute):
        # we need to use the helper functions to set memory values.
        # write only list is a delta sort of thang baby.
        logger.debug("__ses__ making write-only for: %s %s", entity, attribute.name)
        assert(attribute.related_attribute is not None)
#        assert(self is entity.environment)
        assert(attribute.name in entity.attributes)
        assert(entity.attributes[attribute.name] is attribute)
        if attribute.is_set_like:
            logger.debug("__ses__ returning incomplete list.")
            write_only_list = IncompleteSet()
            logger.debug("__ses__ created: %s", write_only_list)
            self._memory_set_value(entity, attribute, write_only_list)
            ret_val = SetHandle(entity, attribute)# write_only_list
        else:
            logger.debug("__ses__ Creating writeonly for: '%s':'%s'",
                attribute, attribute.name)
            entity_class = attribute.related_attribute.entity_class
            from backend.environments.core import Entity
            assert(issubclass(entity_class, Entity))
            write_only = entity_class(environment=self, write_only=True)
            ret_val = write_only
        # Add this request to the 'write_only_connections' set.
        self._add_write_only_connection(entity, attribute, ret_val)
        return ret_val

    def _add_write_only_connection(self, entity, attribute, write_only_value):
        """Adds the entity-attribute that created a write only to a dict that
        stores it in order to replace this locally stored write-only with one
        from a subsequent load.

        """
        try:
            keys_to_attrs = self._write_only_connections[entity.entity_class.__name__]
        except KeyError:
            keys_to_attrs = {}
            self._write_only_connections[entity.entity_class.__name__] = keys_to_attrs
        try:
            attrs_to_write_only = keys_to_attrs[entity.key]
        except KeyError:
            attrs_to_write_only = {}
            keys_to_attrs[entity.key] = attrs_to_write_only
        assert attribute.name not in attrs_to_write_only # You should not be able to make two write-onlies for the same entity-attribute.
        attrs_to_write_only[attribute.name] = write_only_value

    def _adjust_if_write_only_connector(self, entity, attribute, value):
        """If the this entity-attribute pair is the connector for a write-only
        attribute then adjust the session such that the given value (a non
        write-only) is substituted in for the current write-only.

        A replaced write-only Entity will have its Entity object updated such
        that the former write-only Entity behaves as if it is the substitued
        Entity.

        Returns True if a substitution occurred, False otherwise.

        """
        # TODO: could a write only entity keep its attrs on it, keep its
        # own record of pending changes on it, so when needed it can just
        # unload those changes onto the new guy.  (can it use GC to see who
        # is keeping its refs?)
        assert entity.environment == self
        # TODO: This needs to work with the Value-in-Entity system.
        if attribute.is_set_like:
            # This is not yet implemented.
            return False

        # HMmmm... what about write-onlies of write-onlies? dang. TODO
        try:
            keys_to_attrs = self._write_only_connections[entity.entity_class.__name__]
            attrs_to_write_only = keys_to_attrs[entity.key]
            write_only = attrs_to_write_only[attribute.name]
        except KeyError:
            return False# Nothing found, so nothing to adjust.

        # Before we replace, we need to substitute the new object in the
        # instances_by_class so any already-set values can be
        # referred to by the real handle.

        old_value = getattr(entity, attribute.name)

        try:
            keys_to_values = self.values[old_value.entity_class.__name__]
            old_values_dict = keys_to_values[write_only.key]
            try:
                new_values_dict = keys_to_values[value.key]
            except KeyError:
                new_values_dict = {}
                keys_to_values[value.key] = new_values_dict

            #Update new_values_dict with old_values_dict,
            #excluding key attribute in old values
            key_attribute = value.get_key_attribute()
            if key_attribute.name in new_values_dict:
                try:
                    del old_values_dict[key_attribute.name]
                except KeyError:
                    #didn't have it, so don't worry about it
                    pass

            new_values_dict.update(old_values_dict)

            # Assign the old_values_dict's values to the new value's
            # entity.
            for attrname, attrval in old_values_dict.iteritems():
                object.__setattr__(value, attrname, attrval)
        except KeyError:
            pass

        # Remove the connection from the dict.
        del attrs_to_write_only[attribute.name]

        # Remove old object key from values dict
        del keys_to_values[write_only.key]

        object.__setattr__(entity, attribute.name, value)

        # Replace this write-only with the given value.
        write_only._substitute(value)
        return True

    def load(self, sources=None, pipeline=None, suppress_misses=False,
             refresh=True):
        """Load data from the given sources into this Session.

        sources -- Sources to load. (MainPersistent, Caches, etc.)
        pipeline -- Redis pipeline to use. Results are returned.
        suppress_misses -- If True, missing and unitializable Caches will not
           raise NullEnvironmentError, but will cause an Entity to exist in the
           session
        refresh -- If True on a miss these Caches refresh from SQL to Redis.

        Returns the pipeline result, or if pipeline is None, returns [].

        """
        assert not self.session_closed

        # Groom the inputs.
        if sources is None:
            sources = []
        # if a non-list was given, listatize it.
        list_types = (set, list, frozenset,)
        if not isinstance(sources, list_types):
            sources = [sources]

        # Here we make sure to load non-Caches first. (Ex: MainPersistent)
        # Find all the non-Cache sources.
        from backend.environments.cache import Cache
        not_cache = [e for e in sources if not isinstance(e, Cache)]

        # Add them to the session.
        [e.add_to_session(self) for e in not_cache]

        # Remove the non-cache sources as well as any Caches we already have.
        dones = set(not_cache)  # We just added these.
        haves = self.caches  # We added these on a previous load.
        caches = [e for e in sources if e not in dones and e not in haves]

        # Return if there is nothing to do.
        load_count = len(caches)
        if pipeline:
            load_count += len(pipeline.command_stack)
        if 0 == load_count:
            return []

        # Begin the bulk-load procedure.
        # First, some good logging.
        start_time = datetime.now()
        loads = []
        if caches:
            loads.append("   %s" % '\n   '.join([str(e) for e in caches]))
        if pipeline:
            loads.append("redis\n   %s" % '\n   '.join([str(c) for c in pipeline.command_stack]))
        if load_count < 3:
            logger.debug(('load %s' % ', '.join(loads)).replace('\n   ', ' ').replace('   ', ''))
        elif loads:
            logger.debug('load\n%s' % '\n'.join(loads))

        # Separate into shallow and deep caches.
        shallow_caches = []
        deep_caches = []
        for c in caches:
            if c.shallow:
                shallow_caches.append(c)
            else:
                deep_caches.append(c)

        # Get the set of keys to MGET for Cache loading.
        shallow_keys = [c.get_redis_key() for c in shallow_caches]
        deep_keys = [c.get_redis_key() for c in deep_caches]

        # Get results from redis.
        if pipeline:
            command_count = len(pipeline.command_stack)
            redis = pipeline
        else:
            command_count = 0
            redis = get_redis().pipeline()
        assert shallow_keys or deep_keys or command_count
        if deep_keys:
            redis.mget(deep_keys)
        if shallow_keys:
            for key in shallow_keys:
                redis.hgetall(key)
        results = redis.execute()
        
        pipeline_return_value = results[:command_count]
        if deep_keys:
            deep_results = results[command_count]
            shallow_results = results[command_count + 1:]
        else:
            deep_results = []
            shallow_results = results[command_count:]

        # We call a second function here. This second function can be called
        # from somewhere else, giving greater access to load-like behavior.
        # (example, say you got redis results from some other means.)
        return self._load_phase_2(
            deep_sources=deep_caches, deep_results=deep_results,
            shallow_sources=shallow_caches, shallow_results=shallow_results,
            start_time=start_time, suppress_misses=suppress_misses,
            return_value=pipeline_return_value, refresh=refresh)

    def _load_phase_2(self,
        deep_sources=[], deep_results=[], shallow_sources=[], shallow_results=[],
        start_time=None, suppress_misses=False, return_value=None,
        refresh=True):
        """Second part of load, separated out so that one may pass in source
        data from an arbitrary source or a special load and still have the same
        behavior as a load.

        A common usage is to pass in fetched Cache data, which may be None.
        If date is None it will be refreshed as if a load had fetched a None
        from redis.

        (these are old... TODO)
        cache_results -- A list of pickles and or Nones aligned to sources.
        cache_sources -- A list of cache objects aligned to cache_results.
        start_time -- Time the load started, used for logging.
        return_value -- Value to return.
            (load uses this to return piggybacked redis commands.)
        
        """
        if not start_time:
            start_time = datetime.now()
        return_value = return_value or []
        # First run through the results, if there is a None, add a None to
        # found_caches and the empty Cache to init_caches.
        # If there is an encoding, decode it, and add the cache to
        # found_caches.
        found_deeps = [] # List of all caches in order, with Nones for misses.
        init_deeps = [] # List of caches, in order, that were misses.
        # (may contain a (cache, RedisLoadError,) tuple)
#        from model.caches import EntryCache  # (for special case inside loop.)
        for cache, result in izip(deep_sources, deep_results):
            if not result:
                # If there was nothing in redis, mark a None in caches.
                found_deeps.append(None)
                # And add to the list of caches that require initialization.
                init_deeps.append(cache)
            else:
                try:
                    if cache.patches_on_load:
                        c = cache.patch_from_encoding(result)
                        found_deeps.append(c)
                    else:
                        c = cache._depickle(result)
                        c.write_to = False
                        found_deeps.append(c)
                except RedisLoadError, e:
                    # There was something wrong with the pickle.
                    found_deeps.append(None)
                    # If there are appends, we need to keep track of them.
                    if len(e.args) == 2:
                        assert cache.appends_on_write, "Found baseless appendables in a non appens_on_write Cache %s" % self
                        logger.debug("Found an appending Cache with appendables but no base.")
                        init_deeps.append((cache, e))
                    else:
                        init_deeps.append(cache)

        found_shallows = []
        init_shallows = []
        for cache, result in izip(shallow_sources, shallow_results):
            if not result or '_init' not in result:
                # If there was nothing in redis, mark a None in caches.
                found_shallows.append(None)
                # And add to the list of caches that require initialization.
                init_shallows.append(cache)
            else:
                try:
                    # Note: All shallow caches patch on load.
                    cache.patch_from_encoding(result)
                    found_shallows.append(cache)
                except RedisLoadError, e:
                    # There was something wrong with the pickle.
                    found_shallows.append(None)
                    init_shallows.append(cache)

        # Initialize the missing deep caches.
        inited_deeps = []
        if init_deeps:
            logger.debug('   initing %s', pluralize(init_deeps, 'deep cache'))
        for c in init_deeps:
            appendables = None
            if isinstance(c, tuple):
                logger.debug("Gathering post-init appendables")
                appendables = c[1].args[1]
                c = c[0]
            if c.patches_on_load:
                # TODO: We want to do this through the cache, and have it
                # return something saveable, etc.
                # I think there is too much cache-awareness in the session,
                # instead there should be session-awareness in the cache.
                peer = MainPersistent().get(c.entity_class, c.entity_key)
                if peer is None:
                    logger.error("Couldn't find peer in MainPersistent %s", c)
                    if suppress_misses:
                        e = self.fetch_peer(c.top_entity)
                        object.__setattr__(e, 'load_failed', True)
                        env = None
                    else:
                        raise NullEnvironmentError(u'%s' % c)
                else:
                    self.promote_like_cache(peer, c)
                    env = c
            else:
                env = c.get_initialized_cache()
                if env is None:
                    logger.warn("Couldn't get initialized %s", c)
                    if suppress_misses:
                        e = self.fetch_peer(c.top_entity)
                        object.__setattr__(e, 'load_failed', True)
                    else:
                        raise NullEnvironmentError(u'%s' % c)

            if env:
                if refresh:
                    env.requires_saving = True
                    # Mark the cache to be written to redis synchronously.
                    env.save(session=self)
                # NOTE: There is a race here, if the cache gets inited more than
                # once, and then saves out-of-order. (this is a risk whether or
                # not we use sync or async, but is less of a risk with sync.)

                if appendables:
                    logger.debug("Appling post-init appendables")
                    env.apply_appendables(appendables)

            inited_deeps.append(env)

        # Initialize the missing shallow caches.
        inited_shallows = []
        if init_shallows:
            logger.debug('   initing %s', pluralize(init_shallows, 'shallow cache'))
        for c in init_shallows:
            # TODO: We want to do this through the cache, and have it
            # return something saveable, etc.
            # I think there is too much cache-awareness in the session,
            # instead there should be session-awareness in the cache.
            peer = MainPersistent().get(c.entity_class, c.entity_key)
            if peer is None:
                logger.error("Couldn't find peer in MainPersistent %s", c)
                if suppress_misses:
                    e = self.fetch_peer(c.top_entity)
                    object.__setattr__(e, 'load_failed', True)
                    env = None
                else:
                    raise NullEnvironmentError(u'%s' % c)
            else:
                # Put the necessary data into the session from MainPersistent.
                # Note: this means that a shallow cache needs to have its inventory
                # inited during init_backend, so it is not example from being in
                # CACHES.
                self.promote_like_cache(peer, c)
                env = c
                env.write_to = True # shallow caches have this False by default.
                if refresh:
                    env.requires_saving = True
                    # Mark the cache to be written to redis synchronously.
                    env.save(session=self)
                # NOTE: There is a race here, if the cache gets inited more than
                # once, and then saves out-of-order. (this is a risk whether or
                # not we use sync or async, but is less of a risk with sync.)

            inited_shallows.append(env)

        # Merge the inited caches back in with the caches.
        caches = []
        i = 0
        for c in found_deeps:
            if c is None:
                inited_deep = inited_deeps[i]
                if inited_deep:
                    caches.append(inited_deep)
                i += 1
            else:
                caches.append(c)
        i = 0
        for c in found_shallows:
            if c is None:
                inited_shallow = inited_shallows[i]
                if inited_shallow:
                    caches.append(inited_shallow)
                i += 1
            else:
                caches.append(c)

        # The patching caches have all written to the Session already,
        # so any normal cache that doesn't patch_on_load needs to be
        # loaded here.
        load = [c for c in caches if not c.patches_on_load]

        # Prepend the loads to the session's loaded_environments.
        if load:
            self.loaded_environments = load + self.loaded_environments

        # Add caches to the set of loaded caches to prevent redundant loads.
        [self.caches.add(c) for c in caches]

        logger.debug("load took %s", stringify(datetime.now() - start_time))
        return return_value

    def save(self, main_persistent=True, cassandra=True,
        exists_persistent=True, autocomplete_detector=True,
        search_detector=True, try_later_on_this_thread_first=False,
        force_third_class=False,
        consistency=None):
        """Apply (or queue to async) all stored deltas, auto-triggred saves,
        and queued refreshes.

        main_persistent -- True for automatic save 'later' to MainPersistent.
        cassandra -- True for automatic save' later' to Cassandra.
        exists_persistent -- True for automatic save 'now' to ExistsPersistent.
        autocomplete_detector -- True for automatic save 'now' to Autocomplete.
        search_detector -- True for automatic save 'now' to Search.
        try_later_on_this_thread_first -- 'later' makes a first attempt to save
            on this thread, but if it fails the retries are run asynchronously
            as usual.
        force_third_class -- force save 'now' of third_class_storage.
            (third class storage is borken!)
        consistency -- Cassandra consistency level.  None for default.
            
        """
        # Check for no-op.
        start_time = datetime.now()
        assert not self.session_closed
        if (len(self.deltas) == 0 and
            len(self.refresh_caches) == 0 and
            len(self.save_now)==0 and
            len(self.save_later)==0 and
            (self._pipeline is None or len(self._pipeline.command_stack) == 0)):
            logger.debug("save is no-op.")
            # No need to process this save, there is nothing to do.
            # (todo: detect force_third_class here when it is fixed.)
            process_now_queue()
            process_later_queue()
            return

        # Prepare a useful log statement.
        log = []
        if len(self.save_now) > 22:
            log.append('%s nows' % len(self.save_now))
        elif self.save_now:
            log.append('now %s' % ', '.join([str(s) for s in self.save_now]))
        if len(self.save_later) > 3:
            log.append('%s laters' % len(self.save_later))
        elif self.save_later:
            log.append('later %s' % ', '.join([str(s) for s in self.save_later]))
        if len(self.refresh_caches) > 3:
            log.append('refresh %s caches' % len(self.refresh_caches))
        elif self.refresh_caches:
            log.append('refresh caches %s' ', '.join([str(s) for s in self.refresh_caches]))
        if self._pipeline and len(self._pipeline.command_stack) > 3:
            log.append('%s redis ops' % len(self._pipeline.command_stack))
        elif self._pipeline and self._pipeline.command_stack:
            s = ', '.join([str(s) for s in self._pipeline.command_stack])
            if len(s) > 30:
                s = '%s...' % s[:27]
            log.append('redis %s' % s)
        if log:
            logger.debug('save %s %s', self.uuid, ', '.join(log))
        else:
            logger.debug('save %s (defaults only)', self.uuid)
        # TODO: make the logging to \n as load does, it looks better when there
        # are more than 3 things.
#        if load_count < 3:
#            logger.debug(('load %s' % ', '.join(loads)).replace('\n   ', ' ').replace('   ', ''))
#        elif loads:
#            logger.debug('load\n%s' % '\n'.join(loads))
            
        # Get the current now and later sets.
        now = self.save_now
        self.save_now = set()
        later = self.save_later
        # go in 'now' only.
        self.save_later = set()

        # Replace instances of Persistent storage objects with their classes.
        # (This is done because only the classes are pickleable.)
        def _replace_in_set(s, c):
            """Replace an instance of class 'c' with class object 'c'."""
            try:
                s.remove(c())
                s.add(c)
            except KeyError:
                pass

        def _replace_now_and_later(kls):
            """Replace instances of this class with this class in now and
            later.

            """
            _replace_in_set(now, kls)
            _replace_in_set(later, kls)

        # Unlike Caches, these storage objects are singletons.
        _replace_now_and_later(MainPersistent)
        _replace_now_and_later(Cassandra)
        _replace_now_and_later(ExistsPersistent)
        # _replace_now_and_later(AutocompleteDetector)
        # _replace_now_and_later(SearchDetector)

        # By default MainPersistent is in 'later'. (unless it's in 'now'.)
        # (default can be disabled by setting main_persistent=False)
        if main_persistent:
            later.add(MainPersistent)
        if cassandra:
            later.add(Cassandra)

        # By default ExistsPersistent is in 'now'. (unless it's in 'later'.)
        if ExistsPersistent in later:
            raise NotImplementedError(u'ExistsPersistent must be saved synchronously.')
        if exists_persistent and ExistsPersistent not in later:
            now.add(ExistsPersistent)
        # By default AutocompleteDetector is in 'now'...
        # if AutocompleteDetector in later:
        #    raise NotImplementedError(u'Autocomplete must be saved synchronously.')
        # if autocomplete_detector and AutocompleteDetector not in later:
        #    now.add(AutocompleteDetector)
        # By default SearchDetector is in 'now'...
        # if SearchDetector in later:
        #     raise NotImplementedError(u'Search must be saved synchronously.')
        # if search_detector and SearchDetector not in later:
        #     now.add(SearchDetector)

        # Anything in both later and now is just in now.
        later = later - now
        # Any optimization hints we may want to pass on.
        hints = {}
        # TODO: Make a better hint system, don't use this dynamic attr.
        if hasattr(self, 'accelerated_save') and self.accelerated_save:
            hints['accelerated_save'] = True
        hints['consistency'] = consistency
        delta_count = len(self.deltas)
        if now or self._pipeline:
            logger.debug("running 'now' updates %s", pluralize(
                                                         delta_count, 'delta'))
            # This raises an exception if there is a save fail.
            Session._update_or_rollback(self, now, hints=hints)
            logger.debug("'now' save took %s", took(start_time))
        else:
            logger.debug("no nows, punting")

        process_now_queue()
        # Get a serializable representation for the later to operate on.
        later = [e.get_serializable_update(self, hints) for e in later]
        later = [e for e in later if e]
        if later or self.refresh_caches:
            if try_later_on_this_thread_first:
                logger.debug("session running 'later' updates. (try_later_on_this_thread_first) - delta count: %s", delta_count)
                # Attempts to save on this thread first, retries on backend
                # update thread.
                Session._update_or_retry(self.uuid, later,
                    self.refresh_caches, _pop_later_queue(), self.save,
                    hints=hints)
            else:
                logger.debug("session queuing 'later' updates to async. - later count: %s", len(later))
                Session._async_update.delay(self.uuid, later,
                    self.refresh_caches, _pop_later_queue(), hints=hints)
        else:
            logger.debug("session has no laters")
            process_later_queue()

        # --- At some point we may allow flushes, but for now, invalidate the
        # session instead of resetting it.

        self.session_closed = True
        return
        # If we ever have a re-savable session, this code is something to keep
        # in mind.
        # # All deltas are now saved.
        # self.deltas = []
        # # All save instructions have been carried out.
        # self.save_now = set()
        # self.save_later = set()

    def get_pipeline(self):
        """Use the session's pipeline and add commands that run on save."""
        if self._pipeline is None:
            self._pipeline = get_redis().pipeline()
        return self._pipeline

    @staticmethod
    def _update_or_rollback(session, environments, caches_to_refresh=[],
                            hints={}):
        """Call update on each environment with 'session' as an argument.

        Used to process the session's 'now' save.

        Errors are raised, no retries attempted.  _update_or_rollback attempts
        to come as close as possible to a transaction.  If one save fails they
        all fail.  Efforts are made to rollback or undo changes.  However at
        present only errors in MainPersistent cause a
        rollback.  Other failures simply raise an exception without rolling
        anything back.  MainPersintent always saves first
        if present; its errors are the most critical.

        Notes:
        * This is only ever used to perform the session's 'now' save, and is
        only ever called from one place, the session's save method.
        * _update_or_rollback handles errors differently than the method used
        for async saves, _update_or_retry.
        * _update_or_rollback recovers from errors by cleanup and re-raising.
        * _update_or_retry recovers from errors by queuing retries.

        """
        # If MainPersistent are in the arguments they apply
        # first. Thus if either of these fail no other writes will be attempted.
        # Convert any type arguments into instances of that type.
        instances = []
        has_main_persistent = None
        caches = []
        for e in environments:
            if isinstance(e, type):
                e = e()
            if isinstance(e, EnvironmentPersistent):
                has_main_persistent = e
            elif isinstance(e, Cache):
                caches.append(e)
            else:
                instances.append(e)

        # Get serialized deltas and see if there are any.
        if has_main_persistent:
            sql_serialized_update = EnvironmentPersistent.get_serializable_update(session, hints)
            if not sql_serialized_update:
                has_main_persistent = False
        if has_main_persistent:
            operation_count = has_main_persistent._begin_update(*sql_serialized_update[1],
                                                                **sql_serialized_update[2])
            if operation_count:
            # TODO: ONly complete if we do something... but count may be brk
                has_main_persistent._complete_update()

        # At this point if these fail, we don't roll back the above, this is not
        # exactly what we want, but rolling those back is not easy.
        # Do a single redis write for caches and the pipeline.
        if session._pipeline or caches: # or force_third_class:
            redis = session._pipeline if session._pipeline else get_redis().pipeline()
#            third_class_pipeline = get_third_class_pipeline_or_none()
#            if third_class_pipeline:
#                logger.debug("add %s" % pluralize(third_class_pipeline.command_stack, 'third class pipeline item'))
#                redis.command_stack += third_class_pipeline.command_stack
#                close_third_class_pipeline()
            for c in caches:
                c.update(session, pipeline=redis)

            if redis.command_stack:
                redis.execute()
            session._pipeline = None
        # Have each instance save this set of deltas.
        for i in instances:
            i.update(session, hints)
        # Once all environments have been applied, refresh caches.
        for cache in caches_to_refresh:
            cache.refresh()

        logger.debug("_update_or_rollback complete--------------------")

    @staticmethod
    def _update_or_retry(session_id, encodings, caches, tasks, called_by,
                         retry=True, hints={}, **kwargs):
        """Run encoded writes, then refresh caches, then give tasks to Celery.
        
        If any errors are raised, queue an asynchronous retry of _async_update
        with only the failed items as arguments.

        Return True if successful, False if a retry was queued or a fatal error
        raised.

        session_id -- uuid of Session whose writes are being written.
        encodings -- Serializable instructions for saves. (id, args, kwargs,)
        caches -- Refresh these Caches from Persistent on success.
        tasks -- Give these tasks to Celery on success.
        called_by -- Function that called this method.
        retry -- True if a retry may be attempted.

        """
        start_time = datetime.now()

        if 'task_retries' in kwargs:
            task_retries = kwargs['task_retries']
        else:
            task_retries = 0

        # Prepare a string useful for logging.
        if called_by != Session._async_update:
            nth = 'pre-try of '
        elif task_retries == 0:
            nth = ''
        else:
            nth = '%s retry of ' % ordinal(task_retries)
        name = '%s_update_or_retry' % nth

        logger.debug("%s %s begin %s %s %s",
            name, session_id, len(encodings), len(caches), len(tasks))

        # Anything that needs retrying goes here.
        retry_encodings = []
        retry_caches = []
        retry_tasks = []

        retry_log = []
        unretryable = []

        for encoding in encodings:
            try:
                storage_id, encoding_args, encoding_kwargs = encoding
                from backend.environments.persistent import STORAGE_ID_TO_STORAGE_CLASS
                env = STORAGE_ID_TO_STORAGE_CLASS[storage_id]
                env = env()
                env.run_serialized_update(*encoding_args, **encoding_kwargs)
            except Exception, ex:
                logger.error(ex)
                logger.exception(ex)
                if hasattr(ex, 'retry') and not ex.retry:
                    logger.error("%s %s %s raised %s, not-retryable.",
                        name, len(encoding), session_id, type(ex))
                    unretryable.append((encoding, ex, sys.exc_info(),))
                else:
                    retry_encodings.append(encoding)
                    retry_log.append((encoding, ex, sys.exc_info(),))
        # You can't refresh caches when you have errors.
        if retry_encodings or unretryable:
            retry_caches = caches
        else:
            for c in caches:
                try:
                    c.refresh()
                except Exception, e:
                    if hasattr(e, 'retry') and not e.retry:
                        logger.error("%s %s %s raised %s, not-retryable.",
                            name, c, session_id, type(e))
                        unretryable.append((c, e, sys.exc_info(),))
                    else:
                        retry_caches.append(c)
                        retry_log.append((c, e, sys.exc_info(),))

        # You can't run tasks when you have errors.
        if retry_encodings or retry_caches or unretryable:
            retry_tasks = tasks
        else:
            if tasks:
                try:
                    _process_async_tasks(tasks)
                except Exception, e:
                    retry_tasks = tasks
                    retry_log.append((retry_tasks, e, sys.exc_info(),))

        # If there is anything to retry, do that.
        if retry_encodings or retry_caches or retry_tasks or unretryable:
            # Something did not save, it could be anything, a system is down,
            # a time-out because an operation was taking too long.

            # How many seconds until this is retried?
            countdown = _get_timeout(task_retries)

            # Put a logging statement together.
            if unretryable:
                tup = unretryable[0]
            else:
                tup = retry_log[0]
            item = tup[0]
            ex = tup[1]
            ex_data = tup[2]
            errors = unretryable + retry_log
            if len(errors) == 1:
                log = "%s %s %s raised %s %s" % (name, len(item), session_id, type(ex), ex)
            else:
                log = "%s raised %s errors\n" % (name, len(errors))
                for environment, exception, exc_data in unretryable:
                    log += "    (unretryable) %s %s\n" % (environment, exception)
                for environment, exc, exc_data in retry_log:
                    log += "    %s %s\n" % (environment, exc)
            # If we can do a retry, do one.
            if retry and not unretryable and task_retries < settings.MAX_RETRIES:
                if '\n' not in log:
                    log += ', '
                t = stringify(timedelta(seconds=countdown))
                log += "retrying in %s with args %s %s %s" % (
                    t, len(retry_encodings), len(retry_caches), len(retry_tasks))
                if task_retries == 2:
                    send_admin_error_email_no_req(log, ex_data)
                    log += " email sent"

                # If this is called by _async_update, we can retry.
                logger.debug(log)
                if called_by == Session._async_update:
                    Session._async_update.retry(
                     args=[session_id, retry_encodings,
                           retry_caches, retry_tasks],
                     kwargs=kwargs, countdown=countdown, exc=ex)
                # Otherwise we need to call _async_update using 'apply_async'.
                else:
                    Session._async_update.apply_async(
                     args=[session_id, retry_encodings, retry_caches, retry_tasks])
            # We can't retry, so send a 'fail' email.
            else:
                if '\n' not in log:
                    log += ', '
                if not retry:
                    log += "fail due to error, retry is disabled"
                elif unretryable:
                    log += "fail due to unretryable error"
                else:
                    log += "fail due to exceeding max retry"
                logger.error(log)
                logger.exception(ex)
                send_admin_error_email_no_req(log, ex_data)

            # Log a warning, if needed.
            done_time = datetime.now()
            duration = done_time - start_time
            if duration > timedelta(seconds=settings.BACKEND_WARNING_TIME_LIMIT):
                dur_str = stringify(duration)
                # This took long enough to warrant a warning.
                log_str = "time warning, %s %s took %s" % (name,
                                                           session_id,
                                                           dur_str)
                logger.warn(log_str)
                # Send the admins an email.
                send_admin_warn_email(log_str)
            return False
        # In this case everything succeded,
        else:
            # Log a timing message and return True.
            opcount = len(encodings) + len(caches) + len(tasks)
            done_time = datetime.now()
            duration = done_time - start_time
            dur_str = stringify(duration)
            logger.debug("%s %s completed %s in %s",
                name, session_id, pluralize(opcount, 'item'), dur_str)
            if duration > timedelta(seconds=settings.BACKEND_WARNING_TIME_LIMIT):
                # This took long enough to warrant a warning.
                log_str = "time warning, %s %s took %s" % (name,
                                                           session_id,
                                                           dur_str)
                logger.warn(log_str)
                # Send the admins an email.
                send_admin_warn_email(log_str)
            return True
    # This is the method Session uses to handle async application
    # of deltas. (If it can live in a class, move it to Session.)
    # Note: BUMP version number in exchange if incompatible change.  If so,
    #   the old version can be left running to drain the queue.  Also add it
    #   to settings.CELERY_QUEUES
    #TODO TODO -- Also, get some help for test mode.
    @staticmethod
    @task(exchange="backend_update_v1",
          routing_key='backend_update_v1',
          serializer='pickle',
          ignore_result=True,
          max_retries=settings.MAX_RETRIES)
    def _async_update(session_id, encodings, caches, tasks, **kwargs):
        """Start the 'later' portion of the backend save.

        Errors are retried.

        When all saves and refreshes have completed successfully the pending
        asynchronous tasks (tasks), are given to celery.

        Note - Retry information may be present in the keyword arguments.

        session_id -- uuid of session from which the save operations get their data.
        encodings -- Encoded Environments that require saving.
        caches -- Caches to refresh from Persistent.
        tasks -- Tasks to enqueue in Celery after all saves complete.

        """
        logger.debug("_async_do_update %s", session_id)
        return Session._update_or_retry(session_id, encodings, caches, tasks,
                                        Session._async_update, **kwargs)

    def add_delta(self, delta):
        """Adds a Delta to the list of pending Deltas."""
        self.deltas.append(delta)
        if len(self.deltas) >= MAX_DELTAS:
            raise TooManyDeltas

    @staticmethod
    def get_append_delta_for_attribute(attribute):
        return APPEND_ATTRIBUTE_TO_DELTA[attribute.__class__.__name__]

    @staticmethod
    def get_discard_delta_for_attribute(attribute):
        return DISCARD_ATTRIBUTE_TO_DELTA[attribute.__class__.__name__]

    @staticmethod
    def get_set_delta_for_attribute(attribute):
        return SET_ATTRIBUTE_TO_DELTA[attribute.__class__.__name__]

    def get_loader(self, loader):
        """Return equivalent instance of loader if it was loaded already."""
        return self.loaders.get(loader.get_session_hash())

    def add_loader(self, loader):
        """Add this loader to this list of loaded Loaders.

        Note that this does not load the Loader, it just adds it to the
        tracking of loaded Loaders so that get_loader will return it if
        a Loader of the same type with the same args and kwargs is checked.

        """
        self.loaders[loader.get_session_hash()] = loader
        
    def __repr__(self):
        return "Session(%s)" % self.uuid

APPEND_ATTRIBUTE_TO_DELTA = {
    'OneToMany' : AppendOneToMany,
    'ManyToMany' : AppendManyToMany
}

DISCARD_ATTRIBUTE_TO_DELTA = {
    'OneToMany' : DiscardOneToMany,
    'ManyToMany' : DiscardManyToMany
}

SET_ATTRIBUTE_TO_DELTA = {
}
