
"""non-canonical storage that mirrors a subset of canonical sources.

A cache is faster than canonical storage.

For canonical storage see backend.environments.persistent

"""

import logging
from collections import Iterable
from datetime import datetime
from uuid import UUID
from time import sleep
import decimal

from django.conf import settings
from celery.decorators import task

from util.cache import (get_redis, serialize, deserialize_list,
    to_pickle, from_pickle)
from util.when import to_hsntime, from_hsntime
from util.helpers import pluralize, grouper
from backend.datastructures import IncompleteSet
from backend.schema import EnumValue
from backend.exceptions import (AttributeNotLoaded, ExcessInventoryError,
    RedisLoadError)
from backend.environments.core import Entity
from backend.environments.memory import _EnvironmentMemory, SetHandle
from backend.environments.persistent import MainPersistent


logger = logging.getLogger("hsn.backend")
SHALLOW_INIT_KEY = '_init'
SHALLOW_VERSION_KEY = '_ver'


class Cache(_EnvironmentMemory):
    """Abstract base class of non-persistent storage.

    A Cache holds a limited amount of data in a fast, pickleable object.

    Session calls add_to_session on this and it then has access to this
    cache of information.

    The data contained in the cache is defined by the make_environment method.
    This method loads data from another Environment, usually MainPersistent,
    into this cache's memory.

    Calling 'save' on this Environment causes a write-back if it was loaded. If
    this Environment was never loaded a write-to is performed. A write-to is
    created by calling make_environment with the session as the source.

    """

    # If the version in redis is not greater than or equal to the version here,
    # old, and is rejected on load as if it were not in redis.
    cache_version = None
    # If patches_on_load is True, in method 'add_to_session', patch session.
    # If patches_on_load is False, in method 'add_to_session', insert at head
    # of session.loaded_environments .
    patches_on_load = False
    # Shallow Caches have restrictions on their inventory, and don't need
    # to be loaded in order to be saved. They can set and increment individual
    # attributes and are kept in a redis hash and not a pickle.
    # Note: All shallow caches have paches_on_load = True
    shallow = False
    inventoried = True # By default a cache is inventoried.
    appends_on_write = False # By default a cache writes-back instead of appends.

    def __init__(self, key):
        # Initialize the memory.
        super(Cache, self).__init__(key)
        # If True, this cache is not loaded from redis.
        self.write_to = not self.shallow
        # Assign the class variable's value to the instance.
        self.instance_cache_version = self.cache_version
        self.requires_saving = False # Used by update to know whether to
        # write to redis.
        if self.appends_on_write:
            self._hidden_appendables = []

    @classmethod
    # This is an init method.
    def from_peer(cls, entity):
        return cls(entity.key)

    def get_redis_key(self):
        """A unique key for this Environment."""
        return self._get_redis_key(self.entity_key)

    @classmethod
    def _get_redis_key(cls, entity_key):
        """Class method to generate a unique Redis key for this entity key."""
        return '%s_%s' % (cls.__name__, cls._entity_key_to_string(entity_key))

    @classmethod
    def _entity_key_to_string(cls, entity_key):
        """Convert the entity_key to the string-form used in a redis key."""
        value_class = cls.entity_class.get_key_attribute().value_class
        if value_class == UUID:
            return entity_key.get_bytes()
        elif value_class == unicode:
            return entity_key.encode('utf-8')
        else:
            logger.warn("%s using default redis key for class %s" % (
                cls, value_class))
            return str(entity_key)

    @classmethod
    def get_lru_key(cls):
        """key for zset that tracks membership of this Cache in Redis."""
        return 'mgmt_%s' % cls.__name__

    @classmethod
    def _get_redis_key_pattern(cls):
        """Class method to generate a pattern for Redis keys for this cache."""
        return '%s_*' % cls.__name__

    def _get_redis(self):
        """return depickled cache from Redis or raise RedisLoadError."""
        key = self.get_redis_key()
        if self.shallow:
            encoded_cache = get_redis().hgetall(key)
            if not encoded_cache:
                raise RedisLoadError
            return encoded_cache
        else:
            pickles = get_redis().get(key)
            if not pickles:
                raise RedisLoadError
            return self._depickle(pickles)

    @classmethod
    def serialize(cls, item):
        if cls.appends_on_write:
            # Use util.cache's serialize, it can be appended to.
            return serialize(item)
        else:
            # If not appendable, just use the pickle and skip that step.
            return to_pickle(item)

#    @classmethod
#    def deserialize(cls, item):
#        # This is never called with a list, so it must just be a pickle.
#        return from_pickle(item)
    deserialize = from_pickle

    @classmethod
    def deserialize_list(cls, item):
        return deserialize_list(item)

    @classmethod
    def _load_appendable_cache(cls, depickled):
        # Find a base cache, if any.
        base_cache = None
        flat_depickled = []
        for depickle in depickled:
                flat_depickled += depickle
        appendables = []
        for depickle in flat_depickled:
            if isinstance(depickle, Cache):
                base_cache = depickle
            else:
                appendables.append(depickle)

        # There is a special case here, if we don't find a base cache
        # we need to include the appendables list in the exception.
        if base_cache is None:
            raise RedisLoadError("Found a cache with appends but no base",
                                 appendables)

        base_cache.apply_appendables(appendables)
        return base_cache

    @classmethod
    def _load_cache(cls, depickled):
        env = depickled[0]
        assert isinstance(env, Cache)
        if env.instance_cache_version < cls.cache_version:
            raise RedisLoadError("Cache is of older version.")
        return env

    # TODO: This is not a good enough name, we need a name that means
    # "do every single part of making a cache object out of this encoding".
    # Also, this method should raise an exception for things that can
    # only patch from an encoding?
    @classmethod
    def _depickle(cls, pickles):
        """Given a pickled string and appendables, depickle and return."""
        if cls.appends_on_write:
            depickled = cls.deserialize_list(pickles)
        else:
            depickled = [cls.deserialize(pickles)]

        if cls.appends_on_write:
            return cls._load_appendable_cache(depickled)
        else:
            return cls._load_cache(depickled)

    def _memory_set_value(self, *args, **kwargs):
        # Note that this does not work for 'appends_on_write' caches, so they
        # use _queue_append to mark them as requiring saving.
        self.requires_saving = True
        return super(Cache, self)._memory_set_value(*args, **kwargs)

    def _queue_appendable(self, appendable):
        assert self.appends_on_write
        self.is_append_save = True
        self._queued_appendables += appendable

    def _get_queued_appendables(self, session):
        # TODO: This is hacked such that appendables are queued in the feed
        # logic. (This saves having to look through session deltas for many
        # deltas and many caches.) Just apply the saved appendable.
        return self._queued_appendables

    def apply_appendables(self, appendables):
        """Apply the set of given appendables to the cache.

        When a cache is loaded what is fetched from redis is a list of
        appendables (one or many of which are a base cache). This is called
        to apply the appendables to the base cache (after loading from redis)
        so that the cache the session uses to read data from has all of the
        appended changes.

        """
        # Because the appendables are unique encoding for each Cache, the
        # Cache subclass must implement this method.
        raise NotImplementedError

    # This is Object cache specific.
    # Note this is for object caches that need trace the continuity to a
    # single object. Other caches may employ other rules.
    def real_should_apply_delta(self, delta_entity, delta_attribute,
                                cache_entity):
        # First see if this attributes appears anywhere in the inventory.
        # Todo : need to set this up. need to track through the autos and
        # mark them marked.
        if delta_attribute in self._inventory_attributes:
#            logger.debug("%s was in inventory attributes" % delta_attribute)
            # Second see if this entity exists in this environment.
            if self.write_to:
#                logger.debug("__ses__ Confirming for write-only cache")
                # In this case we have a cache that was instantiated with an
                # object but it was neither loaded nor had make_environment
                # called on it.
                # TODO - we need a better recursive checker for this.
                # I hacked this in at the last minute.
                if delta_entity.entity_class == self.entity_class and getattr(delta_entity, delta_entity.entity_class.key_attribute_name) == self.entity_key:
                    return True
                # Included all known autoloaded info from this guy. ???
                # We need special operations for this I think.
#                raise NotImplementedError
            else:
                delta_peer = self.get_peer(delta_entity)
                if delta_peer is not None:
                    # If the attribute is autoload, (and entity exists) it applies.
                    # Note: There is an exception here, what if the autoload
                    # is only one way, in such a way as to not be included
                    # in this cache?
                    if delta_attribute.autoload:
                        return True
                    log_inv = self._inventory
                    if log_inv is not None:
                        log_inv = log_inv.keys()
#                    logger.debug("__ses__trying %s %s %s %s" % (delta_entity, delta_attribute, cache_entity, log_inv))
                    # Otherwise use recursion to determine this delta's eligibility.
#                    logger.debug("what is this: %s" % environment._inventory)
                    return self.new_should_apply_delta(delta_entity,
                                                       delta_attribute,
                                                       cache_entity,
                                                       self._inventory)
        return False

    # Note: Inventory is known to be cycle free except when using autoload.
    # We will use a different autoload recursion.
    # Note this is for object caches that need trace the continuity to a
    # single object. Other caches may employ other rules.
    def new_should_apply_delta(self,
                               delta_entity,
                               delta_attribute,
                               cache_entity,
                               inventory):
#        logger.debug("__ses__ nsad?")
        # We trace through the inventory and entity connections.
        # If we run out of inventory, return False.
        # If we run out of continuity in the stored entities, return False.
        # If we find a connection along known to be included in the cache,
        # return True
        log_inv = inventory
        if log_inv is not None:
            log_inv = log_inv.keys()
#        logger.debug("__ses__ new_should_apply_delta %s %s %s %s" % (delta_entity, cache_entity, delta_attribute, log_inv))
        # Confirm the cache_entity.
        assert cache_entity is not None
        if inventory is None:
#            logger.debug("__ses__ new_should_apply_delta inventory None, return False")
            # An inventory of None means autoload, and this method is never
            # called on an autoload attribute.
            return False

        # Are we checking the entity in question?
        if delta_entity == cache_entity:
            # Is the inventory legal here?
            if 'all' in inventory or delta_attribute in inventory:
                return True
        # Recur.
        for attribute, sub_cache in inventory.iteritems():
            if attribute.related_attribute:
                # If it were an autoload we would have caught it by now.
                if sub_cache is not None:
                    value_is_in_memory = False
                    try:
                        env = cache_entity.environment
                        found_value = env._memory_get_value(cache_entity,
                                                                attribute)
                        value_is_in_memory = True
                    except AttributeNotLoaded:
                        pass
                    if value_is_in_memory and found_value:
                        if attribute.is_set_like:
                            vals = found_value
                            # IncompleteSet BS.
                            if isinstance(vals, SetHandle):
                                inner_val = vals._get_value()
                                if isinstance(inner_val, IncompleteSet):
                                    vals = inner_val._contains
                            for v in vals:
                                if self.new_should_apply_delta(delta_entity, delta_attribute, v, sub_cache):
                                    return True
                        else:
                            if self.new_should_apply_delta(delta_entity,
                                                           delta_attribute,
                                                           found_value,
                                                           sub_cache):
                                return True
#        logger.debug("__ses__ new_shouldappleywhtv returning False, fell through recur")
        return False

    @property
    def top_entity(self):
        return self.fetch(self.entity_class, self.entity_key)

    def _delta_is_applicable(self, delta):
        from backend.environments.session import (
            CreateEntity, DeleteEntity, SetAttribute, AppendManyToMany)

        cache_top_entity = self.top_entity

        if isinstance(delta, CreateEntity):
            return False

        if isinstance(delta, DeleteEntity):
            if self.get_peer(delta.entity) is None:
                return False
            else:
                return True

        # If the attribute is not one stored in this
        # cache, we can bail.
        if delta.attribute is not None:
            if delta.attribute not in self._inventory_attributes:
                return False

        # TODO: Inverse problem: what if you set to None
        # and thus make an orphan in the cache?
        # ans: We need to garbage collect.
        # (we'll do GC efficiently by tracking which
        # orphans we may create and only checking those.)
        # Is the value of this Delta in the cache?
#                            logger.debug("__ses__ checking for set attribute")
        applicable = True
        if isinstance(delta, SetAttribute): # TODO: Append, etc...
#                                logger.debug("__ses__ is set attribute")
            if delta.attribute.related_attribute:
#                                    logger.debug("__ses__ special SetAttribute applicability check.")
                cache_entity = self.get_peer(delta.entity)
#                                    logger.debug("__ses__ cache entity is: %s" % cache_entity)
                if cache_entity is None and not delta.attribute.enum:
#                                        logger.debug("__ses__ this is a relationship delta whose entity is not in the cache.")
                    # We are dealing with a delta whose
                    # entity is not in the cache.
                    # Normally this would not be applied,
                    # But in this case we need to see if
                    # the value is in the cache, and if
                    # the inventory would call for this
                    # delta to be applied.
                    value = delta.value
                    cache_values = []
                    if value:
                        if delta.attribute.is_set_like:
                            cache_values = [self.get_peer(v) for v in value]
                        else:
                            cache_values = [self.get_peer(value)]
#                                        logger.debug("__ses__ value(s) is/are %s" % value)
#                                        logger.debug("__ses__ cache value(s) is/are %s" % cache_values)
                    for cache_value in cache_values:
                        if cache_value is not None:
                            # If the value is in the cache, see
                            # if the other side of the value is
                            # in the inventory.
                            needs_setup = self.real_should_apply_delta(
                                            cache_value,
                                            delta.attribute.related_attribute,
                                            cache_top_entity)
                            if needs_setup:
                                applicable = True
#                                                break
#                                        if applicable:
                            # If we get to here, that means
                            # we need to add the missing entity
                            # so that the delta may apply.
                                related_value = delta.entity
                                if delta.attribute.related_attribute.is_set_like:
                                    related_value = [delta.entity]
                                if cache_value is not None:
                                    self.special_setup(delta.entity.environment,
                                                       cache_value,
                                                       delta.attribute.related_attribute,
                                                       related_value)
                            #environment.special_setup(delta.entity, delta.attribute.related_attribute, delta.value)
            else:
#                                    logger.debug("testing applic %s.%s = %s,%s" % (delta.entity, delta.attribute.name, cache_top_entity, environment))
                applicable = self.real_should_apply_delta(delta.entity,
                                                          delta.attribute,
                                                          cache_top_entity)
        return applicable

    def update(self, session, pipeline=None):
        """Write changes associated with this Cache from 'session' to redis.

        session -- Session whose (applicable) deltas we write to redis.
        pipeline -- The redis pipeline to use for our writes.

        """
        if __debug__:
            logger.debug("%s.update(%s)" % (
                self, pluralize(session.deltas, 'delta')))
        redis = pipeline if pipeline else get_redis().pipeline()
        # Note: Big warning! Deltas do not apply to appending caches
        # automatically.
        # Note: appending caches --
        # This is still a hack because we haven't found an efficient way of
        # getting this data into the right cache. We don't want to do an
        # O(deltas * caches) operation to know where to put appendables.
        # As a result, any application that wants to use the appending save
        # needs to make a cache object, add appendables to it using its
        # add-appendables functions, and call save on it.

        # Appendable-save caches are a special case, we check them here.
        if self.appends_on_write:
            # Accumulate the appendables that we will add to Redis.
            # Note: in an appendable cache everything is appended to Redis,
            # even the base cache.
            append_to_cache = []

            # If the cache was initialized by this session, we need to append
            # an initialized version of this cache.
            if self.requires_saving:
                # Set to False so on load it is properly marked.
                self.requires_saving = False
                # A 'write_to' means the Cache was created locally and is
                # intended to save using data already loaded in the session.
                if self.write_to:
                    # Get a peer from the session.
                    session_peer = session.fetch(self.entity_class,
                                                 self.entity_key)
                    assert session_peer, u"%s is missing %s's top entity" % (
                        session, self)
                    # Add all the necessary information to this Cache object.
                    self.promote_inventory(session_peer)
                # Append this Cache object to appendables we will add to Redis.
                append_to_cache.append(self)
            # If the application uses append save, it marks this special
            # variable.
            if hasattr(self, 'is_append_save') and self.is_append_save:
                # Add all appends queued to the Cache to the appends list.
                append_to_cache += self._get_queued_appendables(session)
            # We've added the base cache if needed and any appends.
            # If we actually have something, serialize it and append to Redis.
            if append_to_cache:
                new_pickle = self.serialize(append_to_cache)
                logger.debug("__erc__ appending to redis: %s %s" % (
                    self.get_redis_key(), type(new_pickle)))
                redis.append(self.get_redis_key(), new_pickle)
                redis.zadd(self.get_lru_key(), **{
                           self._entity_key_to_string(self.entity_key):
                           to_hsntime(datetime.utcnow())})
                redis.expire(self.get_redis_key(), 315569259)
        elif self.shallow:
            # We need to know if we are writing or updating.
            # We write if we had a miss, or if the object was created.
            # Otherwise we update.
            top = session.fetch(self.entity_class, self.entity_key)
            # If this is newly created, then we set everything.
            # (it would be possible to detect non-default deltas, but it is
            # easier and quicker to do no detection and just set everything.)
            if top.created:
                vals = {SHALLOW_INIT_KEY: True,
                        SHALLOW_VERSION_KEY: str(self.instance_cache_version)}
                # NOTE: This approach has race issues.
                # hincrby, however, defaults a missing key to 0. nice!
                # In any case, we clobber. Load first to avoid.
                for attribute in self._inventory_attributes:
                    val = getattr(top, attribute.name)
                    vals[attribute.name] = self.serialize_value(val, attribute)
                redis.hmset(self.get_redis_key(), vals)
                redis.zadd(self.get_lru_key(), **{
                           self._entity_key_to_string(self.entity_key):
                           to_hsntime(datetime.utcnow())})
                redis.expire(self.get_redis_key(), 315569259)
            elif self.write_to:
                # If this is a 'write_to', it means that we initialized the
                # cache from SQL and are now writing it into Redis.  In the
                # case that there is a race with another init, we use hsetnx
                # on any field that does not have a Delta.  If the Delta is a
                # set, then we do a regular hset instead of an hsetnx.  If the
                # Delta is an increment we do an hsetnx of the initialized
                # value and we do an hincr.  Note that there is some fancy code
                # here incase there are multiple sets and increments on the
                # same attribute in this session.
                from backend.environments.deltas import SetAttribute, IncrementAttribute
                deltas = {}
                for delta in session.deltas:
                    if delta.entity == self.top_entity and \
                       delta.attribute in self._inventory_attributes and \
                       isinstance(delta, (SetAttribute, IncrementAttribute)):
                        try:
                            delta_list = deltas[delta.attribute.name]
                        except KeyError:
                            delta_list = []
                            deltas[delta.attribute.name] = delta_list
                        delta_list.append(delta)
                key = self.get_redis_key()
                redis.hset(key, SHALLOW_INIT_KEY, True)
                redis.hset(key,
                           SHALLOW_VERSION_KEY,
                           str(self.instance_cache_version))
                for attribute in self._inventory_attributes:
                    if attribute.name in deltas:
                        delta_list = deltas[attribute.name]
                        # Remove any elements in the delta list that precede
                        # the latest 'set' delta.
                        base_set_index = len(delta_list) - 1
                        for delta in reversed(delta_list):
                            if isinstance(delta, SetAttribute):
                                break
                            base_set_index -= 1
                        if -1 == base_set_index:
                            # This means there was no set in the delta list,
                            # so we do a setnx with the current val minus
                            # the increments.
                            set_val = getattr(top, attribute.name)
                            incr_val = sum([d.value for d in delta_list])
                            if incr_val:
                                set_val = set_val - incr_val

                            encoded = self.serialize_value(set_val, attribute)
                            redis.hsetnx(key,
                                         attribute.name,
                                         encoded)
                            if incr_val:
                                redis.hincrby(key, attribute.name, incr_val)
                        else:
                            # There is a set value, so we will set that value
                            # plus any subsequent increments.
                            set_val = delta_list[base_set_index].value
                            incrs = delta_list[base_set_index + 1:]
                            if incrs:
                                incr_val = sum([d.value for d in incrs])
                                set_val += incr_val
                            redis.hset(key,
                                       attribute.name,
                                       self.serialize_value(set_val, attribute))
                    else:
                        val = getattr(top, attribute.name)
                        redis.hsetnx(key,
                                     attribute.name,
                                     self.serialize_value(val, attribute))
                redis.zadd(self.get_lru_key(), **{
                           self._entity_key_to_string(self.entity_key):
                           to_hsntime(datetime.utcnow())})
                redis.expire(self.get_redis_key(), 315569259)
            # Else just do updates.
            else:
                # See if there was an upgrade, if so, add the upgrade commands.
                if hasattr(self, '_upgrade_pipeline'):
                    redis.command_stack += self._upgrade_pipeline.command_stack
                from backend.environments.deltas import SetAttribute, IncrementAttribute
                did_something = False
                key = self.get_redis_key()
                for delta in session.deltas:
                    if delta.entity == self.top_entity:
                        # Note we have a more naive inventory usage.
                        if delta.attribute in self._inventory_attributes:
                            if isinstance(delta, SetAttribute):
                                logger.debug("Shallow update SETTING")
                                attr = delta.attribute
                                value = delta.value
                                redis.hset(key,
                                           attr.name,
                                           self.serialize_value(value, attr))
                                did_something = True
                            elif isinstance(delta, IncrementAttribute):
                                logger.debug("Shallow update INCREMENTING")
                                attr = delta.attribute
                                value = delta.value
                                redis.hincrby(key,
                                              attr.name,
                                              self.serialize_value(value, attr))
                                did_something = True
                if did_something:
                    redis.zadd(self.get_lru_key(), **{
                               self._entity_key_to_string(self.entity_key):
                               to_hsntime(datetime.utcnow())})
                    redis.expire(self.get_redis_key(), 315569259)
        else:
            # Else not self.appends_on_write, this is not an appending Cache.

            # We are going to save this object to Redis, plus any deltas.
            to_save = self
            # If this cache has not been loaded, we don't need to filter and
            # apply the deltas, we can just call make_environment on the
            # session.
            # (When the cache is being saved without being loaded from SQL it
            # is known as a 'write-to' cache. Common when an item is created.)
            if self.write_to or self.patches_on_load:
                # A note here: notice how we are now going to always save
                # a patches_on_load cache that we called 'save' on.
                # Unlike the non-patching cache that can detect deltas.
                # (which is also expensive.) Backend needs both, but could
                # they be more efficiently de-coupled? Is there a way to
                # know when which would be better?
                session_peer = session.fetch(self.entity_class,
                                             self.entity_key)
                assert session_peer, u"%s missing %s top entity" % (session,
                                                                    self)
                self.promote_inventory(session_peer)
                self.requires_saving = True
            else:
                # This cache was loaded, so apply deltas to it.
                for delta in session.deltas:
                    # Confirm this delta is applicable to this environment.
                    applicable = self._delta_is_applicable(delta)

                    if applicable:
                        logger.debug("__ec__ applying delta %s to %s" % (delta,
                                                                         self))
                        delta.apply(self)
                        self.requires_saving = True
                        from backend.environments.session import (SetAttribute,
                                                              AppendManyToMany)
                        if isinstance(delta, (SetAttribute, AppendManyToMany)) and delta.attribute.related_attribute and not delta.attribute.enum and delta.value is not None and delta.value != []:
                            # Confirm all necessary data is in the Cache.
                            self.special_setup(session, delta.entity,
                                               delta.attribute, delta.value)
                            related_value = delta.entity
                            if delta.attribute.related_attribute.is_set_like:
                                related_value = [delta.entity]
                            cache_value = []
                            if delta.value:
                                if delta.attribute.is_set_like and isinstance(
                                                          delta, SetAttribute):
                                    cache_value = delta.value
                                else:
                                    cache_value = [delta.value]
                            for v in cache_value:
                                # Reverse special setup.
                                self.special_setup(session,
                                                   v,
                                                   delta.attribute.related_attribute,
                                                   related_value)
            # At this point we've applied deltas, and seen if a save is needed.
            # If a save is needed, pickle up and set in Redis.
            if to_save.requires_saving:
                logger.debug('__erc__ _saving_to_redis: %s' % self)
                # Set these to False so it is correct when loaded.
                to_save.write_to = False
                to_save.requires_saving = False
                new_pickle = self.serialize(to_save)
                logger.debug("__erc__ sending to redis: %s %s" % (
                    self.get_redis_key(), type(new_pickle)))
                redis.set(self.get_redis_key(), new_pickle)
                redis.zadd(self.get_lru_key(), **{
                           self._entity_key_to_string(self.entity_key):
                           to_hsntime(datetime.utcnow())})
                redis.expire(self.get_redis_key(), 315569259)
        if pipeline is None:
            redis.execute()

    # Not for application use. This is for setting up testing caches.
    @classmethod
    def init_redis(cls, obj, pipeline=None):
        """Create a cache from the given obj and put it in Redis."""
        redis = pipeline if pipeline else get_redis()
        if cls.shallow:
            vals = {SHALLOW_INIT_KEY: True,
                    SHALLOW_VERSION_KEY: str(cls.cache_version)}
            # NOTE: This approach has race issues.
            # hincrby, however, defaults a missing key to 0. nice!
            # In any case, we clobber. Load first to avoid.
            for attribute in cls._inventory_attributes:
                val = getattr(obj, attribute.name)
                vals[attribute.name] = cls.serialize_value(val, attribute)
            pipeline.hmset(cls._get_redis_key(obj.key), vals)
        else:
            to_save = cls(obj.key).make_environment(obj)
            to_save.write_to = False # If this ever gets loaded, that is.
            to_save.requires_saving = False
            new_pickle = cls.serialize(to_save)
            if cls.appends_on_write:
                redis.append(cls._get_redis_key(obj.key), new_pickle)
            elif cls.shallow:
                raise NotImplementedError("not implemented for shallow caches.")
            else:
                redis.set(cls._get_redis_key(obj.key), new_pickle)
        if not pipeline:
            redis.execute()

    def refresh(self):
        if self.shallow:
            raise NotImplementedError('not implemented for shallow')
        new_pickle = self.serialize(self.get_initialized_cache())
        redis = get_redis().pipeline()
        redis.set(self.get_redis_key(), new_pickle)
        redis.zadd(self.get_lru_key(), **{
                   self._entity_key_to_string(self.entity_key):
                   to_hsntime(datetime.utcnow())})
        redis.expire(self.get_redis_key(), 315569259)
        redis.execute()

    def clear(self):
        """Clear the value redis stores for this Cache."""
        get_redis().delete(self.get_redis_key())

    @classmethod
    def clear_all(cls):
        """Clear all redis-stored caches of this type."""
        r = get_redis()
        [r.delete(k) for k in r.keys(cls._get_redis_key_pattern())]

    def __repr__(self):
        if hasattr(self, 'entity_key'):
            return "%s(%s:%s)" % (self.__class__.__name__,
                                  self.entity_class.__name__,
                                  self.entity_key)
        else:
            return "%s(%s)" % (self.__class__.__name__,
                               self.entity_class.__name__)

    def delete(self, entity):
        # The super method removes 'entity' from relationships.
        super(Cache, self).delete(entity)
        # This method removes 'entity' from memory entirely.
        self._memory_delete_entity(entity)

    def queue_refresh(self):
        from backend import get_active_session
        get_active_session().refresh_caches |= self

    def fetch_environment(self, key=None):
        try:
            return self._get_redis()
        except RedisLoadError:
            logger.debug("__erc__ setting up environment from MainPersistent. %s" % self)
            # Build the environment by looking up in the MainPersistent
            # environment.
            return self.get_initialized_cache()

    # TODO: Why is this in cache and not in memory?
    def promote_inventory(self, peer):
        logger.debug("%s.promote_inventory(%s)" % (self, peer))
        return self._promote_inventory_recur(self, peer, set(), self._inventory)

    @classmethod
    def patch_from_encoding(cls, encoding):
        """Patch an encoding to the session.

        Meant for one to override with a special deserializer.

        """
        from backend import get_active_session
        env = get_active_session()
        if cls.shallow:
            if encoding.get(SHALLOW_INIT_KEY, 'None') != 'True':
                raise RedisLoadError
            # Upgrade replaces the encoding and gives a pipeline to run on
            # Redis to upgrade the cache in Redis.
            upgrade_pipeline = None
            if encoding.get(SHALLOW_VERSION_KEY, 0) != str(cls.cache_version):
                encoding, upgrade_pipeline = cls.upgrade(encoding)
            key = cls.entity_class.get_key_attribute().value_class(encoding[cls.entity_class.key_attribute_name])
            top = env.fetch(cls.entity_class, key)
            for attribute in cls._inventory_attributes:
                if attribute.name != cls.entity_class.key_attribute_name:
                    encoded_val = encoding[attribute.name]
                    decoded_val = cls.deserialize_value(encoded_val, attribute)
# I think this may work, but might have write-only issues?
# put this change on ice because it didn't prove to be faster.
#                    # If the value is not already present on the entity, you
#                    # can put it directly there, otherwise, ignore.
#                    #-----
#                    try:
#                        object.__getattribute__(top, attribute.name)
#                        # If object already has the attr, don't override it.
#                    except:
#                        # If object does not have the attr, set it on the
#                        # object directly.
#                        if not top.write_only:
#                            object.__setattr__(top, attribute.name, decoded_val)
#                        else:
#                            env.promote(top, attribute.name, decoded_val)
#                    #-----
# end upgrade, it would have replaced the following line.
                    # XXX REMOVE THIS HACK WHEN WE FIX NONE ENUMS and INCRs - MEC
                    if decoded_val is None:
                        if attribute.enum:
                            decoded_val = attribute.enum[0]
                        if type(attribute).__name__ == 'Incrementer':
                            decoded_val = 0
                    env.promote(top, attribute.name, decoded_val)
            c = cls(key)
            c.write_to = False
            # If there is an upgrade_pipeline hide it in a dynamic attribute.
            if upgrade_pipeline:
                c._upgrade_pipeline = upgrade_pipeline
                c.save()
            return c
        else:
            c = cls._depickle(encoding)
            env.patch(c)
            return c

    @classmethod
    def upgrade(cls, encoding):
        """Return an upgraded encoding and pipeline or raise RedisLoadError.

        The upgrade encoding is what is put into local memory.  The pipeline
        is used to update Redis on save.

        Must be overriden for upgrade to work.

        Note: You must return an upgrade encoding.

        """
        raise RedisLoadError

    def special_setup(self, session, peer, attribute, value):
        # See update -- this needs a proper name. The idea is that when you
        # include an object into a cache via the given connection it will
        # include the necessary autoload info.
#        logger.debug("special setup: %s %s %s" % (peer, attribute, value))
#        logger.debug("getting inventory entries.")
        if peer is None:
            return
        entity = session.get_peer(peer)
        if entity is None:
#            logger.debug("__ec__ Not special setting up entity because it is not in the cache.: %s" % peer)
            return
        inventory_entries = self.get_inventory_entry_points(session,
                                                            entity,
                                                            attribute,
                                                            value)
        for inventory_entry in inventory_entries:
            logger.debug(inventory_entry)
            # The idea here is not to call
            # setup on the whole dang cache
            # but to just call it on the
            # new object, but from all
            # possible inventory entry points.
            vals = value
#            if attribute.is_set_like:
#                vals = value
#            else:
#                vals = [value]
            if not isinstance(vals, Iterable):
                vals = [vals]
            for v in vals:
                if isinstance(v, Entity):
                    v = session.get_peer(v)
                self._promote_inventory_recur(self,
                                              v, #from SESSION!
                                              set(),
                                              inventory_entry)

    def get_inventory_entry_points(self, session, connector, attribute, new_entity):
        # (Note: new_entity is irrelevant. it is assumed to NOT exist in
        # this environment.)
        # Return a set of inventory sub-caches where this connection will be
        # made.
        # Recur over an inventory, walking down each relation attribute until
        # you reach a cycle to a known entity. Return the set of found
        # inventory sub-caches that will need to be set-up when the new_entity
        # is attached to the connector by way of the attribute.
#        logger.debug("__giep__ connector  %s  attribute  %s" % (connector, attribute))
        return self._get_inventory_entry_points_recur(
            session,
            connector,
            attribute,
            new_entity,
            self.get(self.entity_class, self.entity_key),
            self._inventory,
            set())

    def _get_inventory_entry_points_recur(self,
                                          session,
                                          connecting_entity,
                                          connecting_attribute,
                                          new_entity,
                                          this_entity,
                                          inventory,
                                          searched_with_autoload):
        #logger.debug("__giepr__ %s" % inventory)
        if this_entity is None:
            return []
        if isinstance(this_entity, EnumValue):
            return []
        # If we have reached an autocomplete cycle, and we have checked
        # this_entity for autocomplete already, return the empty set.
        if inventory is None and this_entity in searched_with_autoload:
                return []
        # Set up a local holder for the attributes we will search.
        local_inventory = {}

        # Add the autoload attributes to the local inventory if not already
        # checked.
        if this_entity not in searched_with_autoload:
            for a in this_entity.autoloads:
                local_inventory[a] = None
        # If inventory is specified, include it in the local_inventory.
        if inventory is not None:
            local_inventory.update(inventory)

        # Put this entity in the searched set so we don't search it again.
        # (only if we searched it with autoload only.)
        #if inventory is None: ??
        new_autoload = set()
        new_autoload |= searched_with_autoload
        new_autoload.add(this_entity)
        searched_with_autoload = new_autoload

        retval = []
        for attribute, sub_cache in local_inventory.iteritems():
            # We do not bother with non-relationship attributes.
            if attribute.related_attribute is not None:
                # If this entity is the connector, and this is the connection,
                # return the inventory plus the results of recursion.
                if this_entity == connecting_entity and attribute == connecting_attribute:
#                    logger.debug("__giepr__ appending subcache: %s" % sub_cache)
                    retval.append(sub_cache)
                val = None
                try:
                    # In order to recur, we need to get the next value for
                    # the 'this_entity' argument.
                    val = session._memory_get_value(this_entity,
                                                    attribute,
                                                    generate_incomplete_list=False)
                    if isinstance(val, Entity):
                        assert val.environment == session
                except AttributeNotLoaded:
                    pass

                if attribute.is_set_like:
                    vals = val
                    if isinstance(vals, IncompleteSet):
                        vals = val._contains
                    if vals is None:
                        vals = [] #Kludge... don't know how this can happen?
                else:
                    vals = [val]
                for v in vals:
                    retval += self._get_inventory_entry_points_recur(session,
                                                                     connecting_entity,
                                                                     connecting_attribute,
                                                                     new_entity,
                                                                     v,
                                                                     sub_cache,
                                                                     searched_with_autoload)
#        logger.debug('__giepr__ returning: %s, connecting_entity: %s, this_entity: %s' % (retval, connecting_entity, this_entity))
        return retval


    @classmethod
    def get_all(cls, entity_class, exclude):
        """Returns a list of all attribute names for the class."""
        return_value = []
        for attribute in entity_class.attributes_order:
            if attribute not in exclude:
                if attribute.related_attribute:
                    return_value.append([attribute.name, cls.get_auto(attribute.related_attribute.entity_class, set([attribute]))])
                else:
                    return_value.append(attribute.name)
        return return_value

    def confirm_inventory(self):
        """Confirms that this cache contains values (including autoloads) such
        that the inventory rule is satisfied.

        raises ExcessInventoryError

        Note: This function does not work.

        """
        # Make sure we are holding no forbidden attrs.
        for entity_class_name, keys_to_instances in self.instances_by_class.iteritems():
            for key, instance in keys_to_instances.iteritems():
                attr_names_to_values = self.values[entity_class_name][key]
                for attr_name in attr_names_to_values.keys():
                    attribute = instance.entity_class.get_attribute(attr_name)
                    if attribute not in self._inventory_attributes:
                        raise ExcessInventoryError(self, instance, attribute)
        return True

    def get_initialized_cache(self):
        logger.debug("get initialized cache %s" % self)
        lookup_environment = MainPersistent()
        entity = lookup_environment.get(self.entity_class, self.entity_key)
        if entity is None:
            logger.debug("Could not find peer in MainPersistent, return None")
            return None
        new_environment = self.make_environment(entity)
        self.requires_saving = False
        return new_environment

    @classmethod
    def setup_inventory(cls):
        """Called during backend_init to initialize the inventory system."""
        if cls.shallow:
            if cls.inventory is None:
                cls._inventory = None
                cls._inventory_attributes = set(cls.entity_class.autoloads)
            else:
                cls._inventory = {}
                cls._inventory_attributes = set()
                for attribute_name in cls.inventory:
                    attribute = cls.entity_class.attributes[attribute_name]
                    cls._inventory[attribute] = None
                    cls._inventory_attributes.add(attribute)
        else:
            if cls.inventory is None:
                cls._inventory = None
            else:
                cls._inventory = {}
                cls._setup_inventory(cls.entity_class,
                                     cls.inventory,
                                     cls._inventory)
            cls._inventory_attributes = set()
            cls._setup_inventory_attributes(cls.entity_class,
                                            cls._inventory,
                                            set())

    @classmethod
    def _setup_inventory_attributes(cls, entity_class, inventory, autoloaded):
        if inventory is None:
            if entity_class.__name__ in autoloaded:
                return
            else:
                inventory = {}
        autoloaded.add(entity_class.__name__)

        for attribute in entity_class.autoloads:
            if attribute not in inventory:
                inventory[attribute] = None

        for attribute, sub_cache in inventory.iteritems():
            cls._inventory_attributes.add(attribute)
            if not attribute.enum and attribute.related_attribute:
                cls._inventory_attributes.add(attribute.related_attribute)
                cls._setup_inventory_attributes(
                                      attribute.related_attribute.entity_class,
                                      sub_cache,
                                      autoloaded)

        # Just to be sure - no autoload missing primarys? u sure?
        cls._inventory_attributes.add(entity_class.get_key_attribute())


    @classmethod
    def _setup_inventory(cls, entity_class, inventory, _inventory):
        """Recursive inventory initialization. (see Cache.setup_inventory)"""
        if inventory is None:
            return
        for attribute_name in inventory:
            if attribute_name == 'all':
                for attribute in entity_class.attributes_order:
                    _inventory[attribute] = None
            elif isinstance(attribute_name, list):
                attribute = entity_class.attributes[attribute_name[0]]
                assert attribute.related_attribute
                new_dict = {}
                _inventory[attribute] = new_dict
                cls._setup_inventory(attribute.related_attribute.entity_class,
                                     attribute_name[1],
                                     new_dict)
            else:
                attribute = entity_class.attributes[attribute_name]
                _inventory[attribute] = None


    def make_environment(self, entity):
        """Make an environment based on the Inventory setting."""

        logger.debug("make_environment: '%s'" % entity)
        self.write_to = False
# You can also make an environment from the session.
#        assert isinstance(entity.environment, MainPersistent)
        assert entity.entity_class == self.entity_class
        self.promote_inventory(entity)
        return self

    @classmethod
    def serialize_value(cls, value, attribute):
        """Return value to be stored in redis hash for shallow cache."""
        if attribute.is_set_like:
            raise NotImplementedError(u"Can't use %s in redis hash" % attribute)
        elif issubclass(attribute.value_class, Entity):
            key_attribute = attribute.value_class.get_key_attribute()
            if key_attribute.value_class == unicode:
                raise NotImplementedError(U"Can't use unicode-keyed Entities in redis hash")
            if value is None:
                return ''
            return cls.serialize_value(value.key, key_attribute)
        elif attribute.enum:
            return value.number
        elif attribute.value_class == unicode:
            if value:
                return value.encode('utf-8')
            else:
                return ''
        elif attribute.value_class == bool:
            if value is None:
                return 'None'
            elif value:
                return '1'
            else:
                return '0'
        elif attribute.value_class == datetime:
            return str(to_hsntime(value))
        else:
            return str(value)

    @classmethod
    def deserialize_value(cls, serialized_value, attribute):
        """Return shallow cache value found in redis hash."""
        if attribute.is_set_like:
            raise NotImplementedError(u"Can't use %s in redis hash" % attribute)
        elif issubclass(attribute.value_class, Entity):
            key_attribute = attribute.value_class.get_key_attribute()
            if key_attribute.value_class == unicode:
                raise NotImplementedError(U"Can't use unicode-keyed Entities in redis hash")
            if serialized_value:
                key = cls.deserialize_value(serialized_value, key_attribute)
                return attribute.value_class(key)
            else:
                return None
        elif attribute.enum:
            if serialized_value.lower()=='none':
                return None
            return attribute.enum[int(serialized_value)]
        elif attribute.value_class == unicode:
            return serialized_value.decode('utf-8')
        elif attribute.value_class == str:
            return str(serialized_value)
        elif attribute.value_class == bool:
            if serialized_value == 'None':
                return None
            elif serialized_value == '1':
                return True
            elif serialized_value == '0':
                return False
            else:
                logger.warn("Attempted to deserialized '%s' as boolean" % serialized_value)
                return False
        elif attribute.value_class == datetime:
            if serialized_value.lower()=='none':
                return None
            else:
                return from_hsntime(int(serialized_value))
        elif issubclass(attribute.value_class, (int, long, float, decimal.Decimal)):
            try:
                return attribute.value_class(serialized_value)
            except (ValueError, decimal.InvalidOperation,): #maybe includ TypeError here?
                if serialized_value.lower() == 'none':
                    return None
                else:
                    #Maybe some other type of error that
                    # needs to be handled differently
                    raise
        elif attribute.value_class == UUID:
            if serialized_value.lower()=='none':
                return None
            else:
                return attribute.value_class(serialized_value)
        else:
            return attribute.value_class(serialized_value)

    @classmethod
    def purge(cls, key_strings):
        manager_key = cls.get_lru_key()
        r = get_redis()
        key_template = cls._get_redis_key_pattern()[:-1] + '%s'
        for group in grouper(300, key_strings):
            p = r.pipeline()
            for key_str in group:
                if key_str is None:
                    break
                # Key is bytes if cache-key is UUID based.
                p.delete(key_template % key_str)
                p.zrem(manager_key, key_str)
                # This is so PostCache can also cull some helper structures.
                cls._special_purge(key_str, p)
            p.execute()
            # Prevent redis hosage.
            sleep(.1)

    @classmethod
    def _special_purge(cls, entity_id, pipeline):
        """This is here so PostCache can also cull some helper structures."""
        pass

    @classmethod
    def confirm_management(cls, repair=True):
        """Confirm all extent Caches are listed in their zsets.

        repair -- If True, fix zset based on Caches in Redis."""
        # Get the entity_keys for the excess caches
        manager_key = cls.get_lru_key()
        r = get_redis()
        keys = set(r.keys(cls._get_redis_key_pattern()))
        zset = r.zrevrange(manager_key, 0, -1)
        for entity_id in zset:
            # Key is already cast to string.
            key = '%s%s' % (cls._get_redis_key_pattern()[:-1], entity_id)
            try:
                keys.remove(key)
            except KeyError:
                logger.error('%s in zset, not in Redis' % key.__repr__())
                if repair:
                    r.zrem(manager_key, entity_id)
        for key in keys:
            logger.error('%s in Redis, not in zset' % key.__repr__())
            if repair:
                r.zadd(manager_key, **{key: to_hsntime(datetime.utcnow())})

    def __hash__(self):
        h = str('__env__%s_%s_%s' % (self.__class__.__name__,
                                     self.entity_class.__name__,
                                     self.entity_key)).__hash__()
        return h

    def __eq__(self, other):
        try:
            return self.__hash__() == other.__hash__()
        except AttributeError:
            return False

@task(exchange='manage_caches_v1', serializer='pickle', ignore_result=True)
def manage_caches_task():
    """Celerybeat task for regular purging of Caches in Redis."""
    logger.debug('manage_caches_task')
    manage_caches()

def manage_caches():
    """Trim all cache management zsets and delete excess Caches."""
    import model.caches
    for cache in model.caches.CACHES:
        cache.purge_excess()

def confirm_management(repair=True):
    """Confirm all extent Caches are listed in their zsets."""
    import model.caches
    for cache in model.caches.CACHES:
        cache.confirm_management(repair=repair)
