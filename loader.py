
"""A coroutine based shared-resource loader.

Loaders:
* share script initialization (so a request that calls a script N times in a
  pipeline will only init that script once, not N times, on a miss.)
* use redis locks for inits, so that no two instances are attempting to init
  the same cache at the same time.  (cache-storm protection)
* share a pool of sql permissions on a load, allowing only 15 inits at a time.
  This is cache-storm protection.  Loaders that were denied permission to init
  will reattempt the load after other loaders have finished initing, and will
  request more permission to init from SQL as needed.  The Loaders loop until
  all data is available or they time out.

Shared load happens when a group of loaders are loaded together.
ex: load([FooLoader('key1', 0, 20), FooLoader('key2', 2, 5), BarLoader('key3')])

Each of those loaders has a loader coroutine that is called.  That coroutine
will yield requests in the form of predefined constants, like
'PIPELINE_REQUEST' or 'SQL_INIT_REQUEST'.  They will be called back with
the requested resource, like a redis pipeline or a boolean indicating if
their SQL init permission was granted.  The constants are in this module, each
ending with _REQUEST.  It is in this way that the loader can share resources
across a set of unrelated Loaders.

Note that it is not likely one will need to write a loader as each of the
common Cache classes have Loader implementations.  If one must, see the
loaders in backend.caches for examples of how to write a loader coroutine.

see backend.loader.HookLoader to make a basic redis pipelined loader with a
fetcher and a handler, or see backend.loader.Loader if you want to write a
loading coroutine that can share a sql permission pool, request locks,
coordinate script refreshing, or use a no-results redis pipeline
(so that you don't need to siphon your results off an iterator for things to
be correct.)

"""

#note: http://redis.io/commands/eval
#EVALSHA in the context of pipelining
#Care should be taken when executing EVALSHA in the context of a pipelined request, since even in a pipeline the order of execution of commands must be guaranteed. If EVALSHA will return a NOSCRIPT error the command can not be reissued later otherwise the order of execution is violated.
#The client library implementation should take one of the following approaches:
#Always use plain EVAL when in the context of a pipeline.
#Accumulate all the commands to send into the pipeline, then check for EVAL commands and use the SCRIPT EXISTS command to check if all the scripts are already defined. If not, add SCRIPT LOAD commands on top of the pipeline as required, and use EVALSHA for all the EVAL calls.

from datetime import timedelta, datetime, date, time
from itertools import chain, izip
import logging
import os
import random
from types import GeneratorType

# TODO should not be importing settings here!
from django.conf import settings
from redis import ResponseError
from ujson import dumps, loads

from backend.core import _class_repr, get_active_session
from backend.exceptions import NullEnvironmentError, UninitializedLoader
from util.cache import get_redis
from util.cache.lock import release_lock_script
from util.cassandra import Fetcher
from util.helpers import coroutine, grouper_nofill
from util.when import now_hsntime, from_hsntime, now, stringify, \
    from_hsndate, from_timestorage


logger = logging.getLogger('hsn.backend.caches')
# Use no more than these many keys in any single IN clause.
MAX_IN_CLAUSE = int(settings.BACKEND_MAX_CASSANDRA_IN_CLAUSE)
# Maximum number of inits that may be attempted simultaneously.
# (this should be a setting methinks.)
MAX_INITS = 14
PIPELINE_REQUEST = 'PIPELINE_REQUEST'
SQL_PERMISSION_REQUEST = 'SQL_PERMISSION_REQUEST'
WAIT_REQUEST = 'WAIT_REQUEST'
RESULTS_REQUEST = 'RESULTS_REQUEST'
SQL_INIT_REQUEST = 'SQL_INIT_REQUEST'
NO_RESULTS_PIPELINE_REQUEST = 'NO_RESULTS_PIPELINE_REQUEST'
NO_RESULTS_REQUEST = 'NO_RESULTS_REQUEST'
SCRIPT_INIT_REQUEST = 'SCRIPT_INIT_REQUEST'
CASSANDRA_SHARED_LOAD = 'CASSANDRA_SHARED_LOAD'
CASSANDRA_SHARED_LOAD_DONE = 'CASSANDRA_SHARED_LOAD_DONE'


class Loader(object):
    """A shared-resource loader.

    Can perform a complex load operation while sharing resources with other
    Loaders.

    Resources shared:
    * redis pipelines
    * a pool of requests for permission to init missing caches from SQL.
    * requests to initialize a LUA script in Redis.

    """
    # stack_coroutine -- coroutine for use in _shared_load.
    def get_stack_coroutine(self, *args, **kwargs):
        raise NotImplementedError
    # Note: The given coroutine must be written for use inside of
    # _stack_send, which means yielding a generator continues with the
    # results of that generator, and that one must yield resource requests
    # instead of just calling get_redis() or init_from_sql().

    def __init__(self, *args, **kwargs):
        """Initialiaze a new Loader.

        Loaders are onetime use only objects, once they have run they can not
        be used to load again.  One must instantiate a new Loader for each use.

        """
        # This is set to True when we are done.
        self.done = False
        # When we are done the result can be found here.
        self.result = None
        self.args = args
        self.kwargs = kwargs
        # This is the instance of the stack_coroutine we will be running.
        load_coroutine = self.get_stack_coroutine(*args, **kwargs)
        # Create the stack for _stack_send to operate on.
        self.stack = [load_coroutine]
        # This is the actual load coroutine, a _stack_send coroutine operating
        # on this Loader, and calling 'send' on the load_coroutine in the
        # stack.  This coroutine yields resource requests to the shared
        # loader, and gets sent back the results of those requests (and sends
        # them on to the load_coroutine, or its descendents, on self.stack)
        self.coroutine = _stack_send(self)
        self.load_failed = False

    def load(self, suppress_misses=False):
        """Calls load on this loader and returns the result."""
        return load(self, suppress_misses=suppress_misses)

    def get_session_hash(self):
        """Returns a hash for the session to test if Loaders are equivalent."""
        if self.kwargs:
            kwargs = frozenset(self.kwargs.items())
            return (type(self).__name__, self.args, kwargs)
        else:
            return (type(self).__name__, self.args)

    def __repr__(self):
        args = [str(a) for a in self.args]
        if self.kwargs:
            ekwargs = ['%s=%s' % kv for kv in self.kwargs.iteritems()]
            args = args + ekwargs
        if args:
            return "%s(%s)" % (_class_repr(self), ', '.join(args))
        else:
            return "%s()" % _class_repr(self)


@coroutine
def _stack_send(loader):
    """Coroutine to run the 'stack' calling system on a Loader's coroutine.

    'stack' calling means "yield a generator, get sent back result of
    that generator."

    This coroutine yields the resource requests that the given loader's
    coroutine yields, and sends back the results it gets sent back from
    that yield.

    """
    # The yield waits on arguments for the generator,
    # Which is the only element in the loader's stack.
    next_args = yield
    # We will send these args to the top coroutine in the stack, if it yields
    # a result (and is the only thing on the stack) then that is the final
    # result, if it yields a resource request then we yield
    # that same resource request (and will call this coroutine again with the
    # response for its request when we get called again), if it yields another
    # coroutine we push this coroutine on to the stack and send to the yielded
    # coroutine and repeat.
    # Notes:
    # * We pass the results of the top generator to the one below it until
    #   we do not yield a generator.
    # * If we were yielded a request, then we yield that
    #   request, leaving the yielding generator on top of the stack.
    # * If we were yielded a value and the stack is empty then we are done.
    while not loader.done:
        top = loader.stack[-1]
        result = top.send(next_args)
        if isinstance(result, tuple):
            next_args = result[1]
            result = result[0]
        else:
            next_args = None

        if result in [PIPELINE_REQUEST,
                      RESULTS_REQUEST,
                      SQL_PERMISSION_REQUEST,
                      SCRIPT_INIT_REQUEST,
                      WAIT_REQUEST,
                      NO_RESULTS_PIPELINE_REQUEST,
                      NO_RESULTS_REQUEST,
                      CASSANDRA_SHARED_LOAD,
                      CASSANDRA_SHARED_LOAD_DONE] or isinstance(result, Loader):
            # Leave the generator on top of the stack and yield result.
            next_args = yield result, next_args
        elif isinstance(result, GeneratorType):
            # Push this new generator onto the stack so it will be
            # called at the next iteration of this loop.
            # (it is called with the same arguments.)
            loader.stack.append(result)
        else:
            # This is a value to be passed to the next generator in the
            # stack, if there is one, otherwise it is the final result.
            loader.stack.pop()
            if loader.stack:
                next_args = result
            else:
                loader.result = result
                loader.done = True
    # We are done, the last thing we done is yield the final result.
    yield loader.result


class HookLoader(Loader):
    """A shared-resource loader that uses a pipeline and a results function.

    Use this loader as a generic hook into the loader system.  It is as
    simple as it gets, give it a function that will operate on a pipeline
    and give it another function that will operate on the results.

    """
    def get_stack_coroutine(self, call_with_pipeline, call_with_results):
        return _hook_load(call_with_pipeline, call_with_results)


@coroutine
def _hook_load(call_with_pipeline, call_with_results):
    # First yield is lru_score, in case you use it.
    yield

    # Request a pipeline.
    pipe = yield PIPELINE_REQUEST
    call_with_pipeline(pipe)

    # Request results.
    results_iter = yield RESULTS_REQUEST
    result = call_with_results(results_iter)

    yield result

# Note: If you want to do anything fancier than that you should write a
# loader coroutine and make a Loader subclass that uses that.
class HookLoaderBase(Loader):
    """A shared-resource loader that uses a pipeline and a results function.

    A subclass should implement the 'call_with_pipeline' and
    'call_with_results' functions.

    """
    call_with_pipeline = None
    call_with_results = None

    def get_stack_coroutine(self):
        return _hook_load(self.call_with_pipeline, self.call_with_results)


class InventorySubLoader(Loader):

    def __init__(self, base_loader, entity, autoloaded, sub_cache, consistency):
        self.consistency = consistency
        self.base_loader = base_loader
        super(InventorySubLoader, self).__init__(base_loader, entity, autoloaded, sub_cache, consistency)

    def get_stack_coroutine(self, base_loader, entity, autoloaded, sub_cache, consistency):
        return _promote_inventory_coroutine(base_loader,
                                            entity,
                                            set(),
                                            sub_cache,
                                            consistency)


class InventoryLoader(Loader):
    """Loads the specified inventory from Cassandra (if needed.)"""
    # Subclasses must override entity_class and inventory.
    entity_class = None
    inventory = None
    consistency = None
    _inventory = None
    _inventory_attributes = set()
    _inventory_initialized = False

    def __init__(self, *args, **kwargs):
        if not self._inventory_initialized:
            raise UninitializedLoader("Add %s to model.caches.LOADERS so that it may be initialized.  (if you've done that already, make sure init_backend is called from your script" % type(self).__name__)
        super(InventoryLoader, self).__init__(*args, **kwargs)

    @classmethod
    def setup_inventory(cls):
        """Called during backend_init to initialize the inventory system."""
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
        cls._inventory_initialized = True

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

    def get_stack_coroutine(self, key):
        self.entity = self.entity_class(key)
        return _promote_inventory_coroutine(self,
                                            self.entity,
                                            set(),
                                            self._inventory,
                                            self.consistency)


class CompositeKeyLoader(InventoryLoader):
    """An InventoryLoader that accesses data by way of composite key."""
    
    # The name (string) of the composite key from entity_class.persistent_class.indexes
    composite_key_name = None

    def get_stack_coroutine(self, keys, start=None, count=None, desc=True):
        return _load_inventory_by_composite_key(self, self.entity_class,
                                                self.composite_key_name,
                                                keys, start, count,
                                                desc, set(),
                                                self._inventory,
                                                self.consistency)
                                                
    def get_session_hash(self):
        """Returns a hashable for the session to test Loader equivalence."""
        cache_class = type(self).__name__
        keys = self.args[0]
        if not isinstance(keys, list):
            keys = [keys]
        keys = frozenset(keys)
        if self.kwargs:
            kwargs = frozenset(self.kwargs.items())
            return (cache_class, keys, kwargs)
        else:
            return (cache_class, keys)


@coroutine
def _load_inventory_by_composite_key(base_loader, entity_class,
                                     composite_key_name,
                                     keys, start, count, desc,
                                     autoloaded,
                                     inventory,
                                     consistency):
    """Loads ordered list of Entities from a composite key(s)

    base_loader -- Loader that invoked this coroutine
    entity_class -- Entity class of Entity that has the composite key
    composite_key_name -- string name as found in entity_class.persistent_class.indexes
    keys -- key or list of keys to use in the where clause of the load
    start -- value to start query with (>=, <= desc depending), or None.
    count -- number of Entities to fetch or None for all
    desc -- True for descending order (highest to lowest), false for ascending
    autoloaded -- set of all entities that have had their autoloads loaded
    inventory -- the parsed (._inventory) inventory that we are loading
    consistency -- the forced consistency of this loader or None for default

    """
    # This needs to fetch certain fields from the composite key, it needs something
    # like:
    # #CREATE TABLE timeline (
#  user_id varchar,
#  tweet_id uuid,
#  author varchar,
#  body varchar,
#  PRIMARY KEY (user_id, tweet_id)
#);
#SELECT * FROM timeline WHERE user_id = gmason
#ORDER BY tweet_id DESC LIMIT 50;
#
    if not isinstance(keys, list):
        keys = [keys]

    if consistency is None:
        consistency = settings.BACKEND_READ_CONSISTENCY
    # SELECT <inventory + autoloads> etc...
    # it's very much like p.i.c. but it will operate on a list of them.
    # however, the initial load will get row data (to cut down on round trips,
    # and make certain operations single-trips if need be)
    # Well... for now let's copy the whole thing and then chop it back and
    # share it as needed.
    # First yield is empty, to start us off.
    from util.log import logger
    lru_score_or_none = yield

    # Get a list of scalar attributes that we will be fetching.
    local_inventory = {}

    for a in entity_class.autoloads:
        local_inventory[a] = None

    # If there is anything in inventory, add it to local_inventory.
    if inventory is not None:
        local_inventory.update(inventory)

    key_attribute = entity_class.get_key_attribute()
    # A list of the attributes to load with the key attribute first.
    scalars = [key_attribute]
    incrementers = []
    for attribute, sub_cache in local_inventory.iteritems():
        if attribute == key_attribute:
            continue
        if attribute.is_set_like:
            raise NotImplementedError, 'loader does not support set like attrs.'
        if type(attribute).__name__ == 'Incrementer':
            incrementers.append(attribute)
        else:
            scalars.append(attribute)

    # This does not use Cassandra shared load because a lookup in a composite
    # key table is not sharable.
    column_family = entity_class.get_index_column_family(composite_key_name)
    attr_names = [attr.name for attr in scalars]
    key_names = entity_class.persistent_class.indexes[composite_key_name]

    key_attr_names = []
    sort_attr_name = None
    for index, attr_name in enumerate(key_names):
        if index < len(keys):
            key_attr_names.append(attr_name)
        else:
            #sort attr is always the last, so
            # if we're here, we stop
            sort_attr_name = attr_name
            break

    fetcher = Fetcher(cf_name=column_family, fields=attr_names, order_by=key_names[1] if sort_attr_name else None, order_by_direction=not desc, limit=count, consistency_level=consistency)
    for key, key_attr_name in izip(keys, key_attr_names):
        attribute = entity_class.get_attribute(key_attr_name)
        if attribute.related_attribute and not attribute.enum and not hasattr(key, 'entity_class'):
            # We allow keys to be given in place of Entities
            encoded_key = key
        else:
            encoded_key = attribute.cassandra_encode(key)
        fetcher.add_column_value_relation(key_attr_name, encoded_key)

    if start:
        if sort_attr_name:
            attribute = entity_class.get_attribute(sort_attr_name)
            sort_attr_value = attribute.cassandra_encode(start)
            sort_attr_comparator = '<=' if desc else '>='
            fetcher.add_column_value_relation(sort_attr_name, sort_attr_value, sort_attr_comparator)
        else:
            raise NotImplementedError('need to implement start arg with complete key set, \
are you giving all the keys?  Try giving the deepest key as the start argument')

    results = fetcher.fetch_all()
    # The result will have a list of results, each a list of values.

    # Here we check if we need to emit any subloaders to load values for
    # objects (should they be non-None in the result) that the inventory
    # requires that we load.
    return_results = []
    for result_values in results:
        result_iter = iter(result_values)
        key = result_iter.next()
        entity = entity_class(key)
        session = entity.environment
        return_results.append(entity)
        autoloaded.add(entity)
        for attr, encoded_value in izip(scalars[1:], result_iter):
            if encoded_value and attr.related_attribute and not attr.enum:
                value = attr.cassandra_decode(encoded_value)
                if session._adjust_if_write_only_connector(entity, attr, value):
                    # if _adjust... returns True, the promotion is
                    # done
                    pass
                else:
                    session.promote(entity, attr.name, value)
            else:
                # XXX REMOVE THIS HACK AFTER WE FIX EXISTING NONE ENUMS - MEC
                if encoded_value is None and not attr.related_attribute and attr.enum:
                    value = attr.enum[0]
                else:
                    value = attr.cassandra_decode(encoded_value)
                session.promote(entity, attr.name, value)
        # These may not be in the inventory, but we know them from the load
        # arguments.  Probably not worth doing, but someday maybe.
#        for key_value, key_attr_name in izip(keys, key_attr_names):
#            if key_attr_name != key_attribute.name:
#                attribute = entity_class.get_attribute(key_attr_name)
#                if attribute.related_attribute and not attribute.enum and not hasattr(key_value, 'entity_class'):
#                    promote_value = attribute.related_attribute.entity_class(key_value)
#                else:
#                    promote_value = key_value
#                session.promote(entity, key_attr_name, promote_value)
            
    # Emit subloaders for increment fields so they can do a shared load.
    if incrementers:
        # fromkeys is shorthand i_i = {incr1attr: None, incr2attr: None, ...}
        increment_inventory = dict.fromkeys([i for i in incrementers])
        for entity in return_results:            
            yield InventorySubLoader(base_loader,
                                     entity,
                                     autoloaded,
                                     increment_inventory,
                                     consistency)
    # Determine if there is any further inventory that requires loading.
    # entity => subinventory.
    subinventories = {}
    for attribute, subinventory in local_inventory.iteritems():
        if type(attribute).__name__ == 'ManyToOne' or type(attribute).__name__ == 'FakeManyToOne':
            for entity in return_results:
                # (We don't have to worry about write-onlies because we just
                # promoted in the value in the previous section.)
                value = getattr(entity, attribute.name)
                if value is not None:
                    existing_sub = subinventories.get(value)
                    if existing_sub is None:
                        subinventories[value] = subinventory
                    elif subinventory is not None:
                        yield InventorySubLoader(base_loader,
                                                 value,
                                                 autoloaded,
                                                 subinventory,
                                                 consistency)
    for value, subinventory in subinventories.iteritems():
        yield InventorySubLoader(base_loader,
                                 value,
                                 autoloaded,
                                 subinventory,
                                 consistency)

    yield return_results


@coroutine
def _promote_inventory_coroutine(base_loader, 
                                 entity,
                                 autoloaded,
                                 inventory,
                                 consistency):
    # First yield is empty, to start us off.
    lru_score_or_none = yield

    # If the inventory argument is None it means we still do autoload,
    # if we haven't already done so.
    if inventory is None and entity in autoloaded:
        return

    # This is a dict of attributes we'll be promoting.
    local_inventory = {}

    # If this entity has not had its most basic attributes promoted we
    # add those attributes to the set of attributes we will promote.
    if entity not in autoloaded:
        for a in entity.autoloads:
            local_inventory[a] = None
        autoloaded.add(entity)

    # If there is anything in inventory, add it to local_inventory.
    if inventory is not None:
        local_inventory.update(inventory)

    attributes_to_load = []
    # See if we need to yield any recurrent loaders (subloaders).
    # We can do this at this point if we have any of these attributes.
    for attribute, sub_cache in local_inventory.iteritems():
        if attribute.is_set_like:
            raise NotImplementedError, 'loader does not support set like attrs.'

        # Do we already have this attribute?
        already_has_attribute = False
        try:
            value = object.__getattribute__(entity, attribute.name)
            try:
                is_write_only = value.write_only
            except AttributeError:
                is_write_only = False
            if not is_write_only:
                already_has_attribute = True
        except AttributeError:
            pass

        # Note that this will not find a write only, as that is kept in the
        # session, but this may change if we change how the session handles
        # certain attributes stored on the Entity.  So always check for
        # each entity in its loader.
        
        # If we have the value, and it is an Entity, we may need to yield
        # another loader.  (Otherwise we need to load the key.)
        if already_has_attribute and attribute.related_attribute and \
           value and (sub_cache is not None or value not in autoloaded):
            yield InventorySubLoader(base_loader,
                                     value,
                                     autoloaded,
                                     sub_cache,
                                     consistency)

        if not already_has_attribute:
            attributes_to_load.append((attribute, sub_cache,))

    if attributes_to_load:
        attrs = [attr for attr, sub_cache in attributes_to_load]
        # A dict of columnfamily -> key -> attrname
        # These will be consolodated and loaded.

        # A dict of entity->set(attrs) to load
        cassandra_to_load = yield CASSANDRA_SHARED_LOAD
        try:
            cassandra_to_load[entity] = cassandra_to_load[entity] | set(attrs)
        except KeyError:
            cassandra_to_load[entity] = set(attrs)

        check_result, misses = yield CASSANDRA_SHARED_LOAD_DONE
        if entity in misses:
            # We were a miss, so let's cease recursion.
            yield entity

        # Results are promoted to their entities by the shared loader.
        # Now we need to recur on newly found related objects.
        for attr, sub_cache in attributes_to_load:
            if attr.related_attribute and not attr.enum:
                value = getattr(entity, attr.name)
                if value and (sub_cache or value not in autoloaded):
                    yield InventorySubLoader(base_loader,
                                             value,
                                             autoloaded,
                                             sub_cache,
                                             consistency)
    yield entity


def load(loaders, lru_score=None, consistency=None, suppress_misses=False):
    """Do a shared-resource load on a set of loader objects.

    loaders -- List of loaders (generators) to load together.
    lru_score -- Converted timestamp for the fetchers to mark LRUs with.
    consistency -- Forced minimum Cassandra consistency.  None for default.
    suppress_misses -- If True, missing and unitializable Loaders will not
        raise NullEnvironmentError, but will cause an Entity to exist in the
        session.

    """
    if not loaders:
        return loaders
    if isinstance(loaders, list):
        return_list = True
    else:
        return_list = False
        loaders = [loaders]

    if lru_score is None:
        lru_score = now_hsntime()

    # See if the session has already loaded these, if so, do not re-load.
    already_have_or_none = []
    need_to_load = []
    session = get_active_session()
    for loader in loaders:
        already_have_loader = session.get_loader(loader)
        if already_have_loader:
            already_have_or_none.append(loader)
            # Need to give this equivalent instance the same result we
            # loaded previously.
            loader.result = already_have_loader.result
        else:
            already_have_or_none.append(None)
            need_to_load.append(loader)

    if need_to_load:
        _shared_load(need_to_load,
                     lru_score,
                     consistency,
                     suppress_misses=suppress_misses)

        # Add the need_to_load loaders to the session.
        for loader in need_to_load:
            session.add_loader(loader)
            
    # Re-integrate the list of loaders from old loaders and loaded loaders.
    loaders = []
    for loader in already_have_or_none:
        if loader is None:
            loaders.append(need_to_load.pop(0))
        else:
            loaders.append(loader)

    retval = [l.result for l in loaders]
    if return_list:
        return retval
    else:
        return retval[0]


def _shared_load(loaders, lru_score, consistency, suppress_misses=False):
    """Runs a set of Loader coroutines concurrently with shared resources.

    loaders -- list of Loaders whose coroutines will be run until completion.
    lru_score -- score with which each Loader will bump its LRU zset.
    consistency -- Cassandra consistency.  None for default.
    suppress_misses -- If True, missing, inconsistent or unitializable Loaders
        will not raise NullEnvironmentError, but will cause an Entity to exist
        in the session.  The top-level Entity of the failed Loader will have
        its .load_failed attr set to True, and will have the Loader added to
        its failed_loaders list.  The missing Entity is not necessarily the
        the top-entity, it could be nested in the Loader's inventory.

    """
    # We maintain the state of the loader with these lists.
    pipeliners = []
    no_results_pipeliners = []
    sql_permission_requesters = []
    script_init_requesters = []
    cassandra_shared_loaders = []
    waiters = []

    def send_to_list(loader_list, *send_args, **send_kwargs):
        """Call 'send' with the given args on each Loader's coroutine ."""
        for loader in loader_list:
            send_to_loader(loader, *send_args, **send_kwargs)

    def send_to_loader(loader, *send_args, **send_kwargs):
        """Call 'send' with the given args on the Loader's coroutine.

        The results of the send must be a resource request or the final
        result.  If the result is a request then the loader is appeneded to
        the appropriate list (pipeliners, no_results_pipeliners, etc.)

        """
        result = loader.coroutine.send(*send_args, **send_kwargs)

        if isinstance(result, tuple):
            result_args = result[1]
            result = result[0]
        else:
            result_args = None

        # Result should be a type of request, another loader, or the final
        # result value.
        if result == PIPELINE_REQUEST:
            pipeliners.append(loader)
        elif result == NO_RESULTS_PIPELINE_REQUEST:
            no_results_pipeliners.append(loader)
        elif result == SQL_PERMISSION_REQUEST:
            sql_permission_requesters.append(loader)
        elif result == WAIT_REQUEST:
            waiters.append(loader)
        elif result == SCRIPT_INIT_REQUEST:
            script_init_requesters.append((loader, result_args))
        elif result == CASSANDRA_SHARED_LOAD:
            cassandra_shared_loaders.append(loader)
        elif isinstance(result, Loader):
            send_to_loader(result, None)
            send_to_loader(loader, None)
        else:
            assert loader.done, \
                'coroutine gave unexpected result %s' % result

    # All of the generators are fresh, so hit them all once, they should each
    # be yielding on a request for an lru_score.
    send_to_list(loaders, lru_score)

    # This is the main loop.
    # We run until no Loader gets appended to any more request lists.
    while pipeliners or \
          sql_permission_requesters or \
          no_results_pipeliners or \
          script_init_requesters or \
          cassandra_shared_loaders or \
          waiters:
        # if pipeliners:
        #     logger.error('pipeliners %s' % len(pipeliners))
        # if sql_permission_requesters:
        #     logger.error('sql_permission_requesters %s' % len(sql_permission_requesters))
        # if no_results_pipeliners:
        #     logger.error('no_results_pipeliners %s' % len(no_results_pipeliners))
        # if script_init_requesters:
        #     logger.error('script_init_requesters %s' % len(script_init_requesters))
        # if cassandra_shared_loaders:
        #     logger.error('cassandra_shared_loaders %s' % len(cassandra_shared_loaders))
        # if waiters:
        #     logger.error('waiters %s' % len(waiters))

        if sql_permission_requesters:
            working_sql_permission_requesters = sql_permission_requesters
            sql_permission_requesters = []
            count = len(working_sql_permission_requesters)
            if count > MAX_INITS:
                # We have more than the max number of simultaneous inits
                # allowed, so distribute inits at random.
                granted_indicies = random.sample(
                    xrange(len(working_sql_permission_requesters)),
                    MAX_INITS)
                # This is a set of indicies.  If an index is in here, it has
                # been granted init permission.  (Note that the coroutine
                # only has permission to seek a lock, not yet permission
                # to init.)
                granted_indicies = frozenset(granted_indicies)

                permission_grants = []
                for i, loader in enumerate(working_sql_permission_requesters):
                    if i in granted_indicies:
                        permission_grants.append(True)
                    else:
                        permission_grants.append(False)
            else:
                permission_grants = [True] * count

            for loader, permission in izip(working_sql_permission_requesters,
                                           permission_grants):
                send_to_loader(loader, permission)
            continue

        scripts_to_init = []
        working_script_init_requesters = []
        if script_init_requesters:
            # Prepare 'scripts_to_init' for the pipeline execute.
            scripts = {}
            for loader, script in script_init_requesters:
                scripts[script] = True
            for script, approval in scripts.iteritems():
                scripts_to_init.append(script)
            working_script_init_requesters = script_init_requesters
            script_init_requesters = []

        if pipeliners or no_results_pipeliners or scripts_to_init:
            # Make a pipe to share with pipeliners, no-result pipeliners
            # and script initers.
            pipe = get_redis().pipeline()
            working_pipeliners = pipeliners
            pipeliners = []
            for loader in working_pipeliners:
                result = loader.coroutine.send(pipe)
                if isinstance(result, tuple):
                    result = result[0]
                if result != RESULTS_REQUEST:
                    raise NotImplementedError, 'Need to keep running after yield of PIPELINE request...?? or is this in stack-send?'
                assert result == RESULTS_REQUEST, 'result was %s' % result

            working_no_results_pipeliners = no_results_pipeliners
            no_results_pipeliners = []
            for loader in working_no_results_pipeliners:
                result = loader.coroutine.send(pipe)
                if isinstance(result, tuple):
                    result = result[0]
                assert result == NO_RESULTS_REQUEST

            for script in scripts_to_init:
                pipe.script('LOAD', script.text)
                scripts_to_init = []
            results = pipe.execute()

            if working_pipeliners:
                results_iter = iter(results)
                send_to_list(working_pipeliners, results_iter)

            if working_no_results_pipeliners:
                send_to_list(working_no_results_pipeliners, None)

            if working_script_init_requesters:
                loaders = [tup[0] for tup in working_script_init_requesters]
                send_to_list(loaders, None)

            continue

        if cassandra_shared_loaders:
            working_cassandra_shared_loaders = cassandra_shared_loaders
            cassandra_shared_loaders = []
            cassandra_to_load = {} # entity->set(attrs)
            # entity_to_loader is for mapping missed entities back to the
            # loaders that requested them.
            entity_to_loaders = {} # entity->set(loaders)
            # Pass a entity->set(attrs) to load dict to get filled in.
            for loader in working_cassandra_shared_loaders:
                # Give a local version of the to-load dict so that we can
                # map any missed rows back to the Loaders that required them.
                local_to_load = {}
                check_result = loader.coroutine.send(local_to_load)
                try:
                    check_result, arg = check_result
                except ValueError:
                    check_result = check_result
                    arg = None
                assert check_result == CASSANDRA_SHARED_LOAD_DONE, \
                       'expected %s, got %s' % (CASSANDRA_SHARED_LOAD_DONE,
                                                check_result)
                # Merge the entity->set(attrs) that the coroutine yielded
                # into the shared entity->set(attrs) structure.
                for entity, attrs in local_to_load.iteritems():
                    # Match each entity to this loader in case there is a miss.
                    try:
                        existing_loaders = entity_to_loaders[entity]
                        existing_loaders.append(loader)
                    except KeyError:
                        existing_loaders = [loader]
                        entity_to_loaders[entity] = existing_loaders
                    # Merge this to-load data into the shared to-load data.
                    try:
                        attrset = cassandra_to_load[entity]
                    except KeyError:
                        attrset = frozenset()
                    cassandra_to_load[entity] = attrset | frozenset(attrs)

            # If consistency is not specified, then see if these loaders
            # have a declared consistency.  If not, use the setting.
            this_consistency = consistency
            if this_consistency is None:
                this_consistency = settings.BACKEND_READ_CONSISTENCY
            if consistency is None:
                for loader in working_cassandra_shared_loaders:
                    if loader.consistency:
                        if is_stricter(loader.consistency, this_consistency):
                            this_consistency = loader.consistency

            # See if we have any requests that have the exact same entity_class
            # and attributes.  If they do we will load them in a single
            # statement using an IN clause.
            # entity_class -> frozenset(attrs to load for entity) -> [entities]
            # Note that we do this separately for scalars and counters.
            by_entity_class_scalars = {}
            by_entity_class_counters = {}
            for entity, attrs in cassandra_to_load.iteritems():
                scalars = []
                counters = []
                for attribute in attrs:
                    if type(attribute).__name__ == 'Incrementer':
                        counters.append(attribute)
                    else:
                        scalars.append(attribute)
                entity_class = entity.entity_class
                entity_class_name = entity_class.__name__
                # scalars.
                if scalars:
                    # The key attribute must be present in the query, so
                    # we add it here to be 100% sure it's in there.
                    # (We don't add it earlier because if only increment
                    # fields are requested then we will only check the primary
                    # table if there is a miss.)
                    scalars.append(entity_class.get_key_attribute())
                    scalars = frozenset(scalars)
                    try:
                        by_attrs = by_entity_class_scalars[entity_class_name]
                    except KeyError:
                        by_attrs = {}
                        by_entity_class_scalars[entity_class_name] = by_attrs
                    try:
                        same_attr_entities = by_attrs[scalars]
                    except KeyError:
                        same_attr_entities = []
                        by_attrs[scalars] = same_attr_entities
                    same_attr_entities.append(entity)
                # counters.
                if counters:
                    counters = frozenset(counters)
                    try:
                        by_attrs = by_entity_class_counters[entity_class_name]
                    except KeyError:
                        by_attrs = {}
                        by_entity_class_counters[entity_class_name] = by_attrs
                    try:
                        same_attr_entities = by_attrs[counters]
                    except KeyError:
                        same_attr_entities = []
                        by_attrs[counters] = same_attr_entities
                    same_attr_entities.append(entity)

            misses = []
            has_scalars = set()
            found_in_scalar_column_family = set()
            session = get_active_session()
            for entity_class_name, by_attrs in by_entity_class_scalars.iteritems():
                for attrs, same_attr_entities in by_attrs.iteritems():
                    # Make a list from frozenset 'attrs' for consistent order,
                    # with the key attribute first.
                    key_attr = entity_class.get_key_attribute()
                    key_name = key_attr.name
                    # Make attrs into a list to preserve order.
                    attrs = list(attrs)
                    # Take out the key attribute.
                    try:
                        attrs.remove(key_attr)
                    except ValueError:
                        pass
                    # Insert key attribute at beginning of list.
                    attrs.insert(0, key_attr)

                    # Some convenient local vars we will refer to.
                    entity_class = same_attr_entities[0].entity_class
                    keys = [getattr(e, key_name) for e in same_attr_entities]

                    # All of these entities have scalar attributes.
                    # We keep track of these facts because it needed for
                    # dealing with misses in the counter table.
                    for entity in same_attr_entities:
                        has_scalars.add(entity)

                    # Do the multi load from Cassandra.
                    groups = grouper_nofill(MAX_IN_CLAUSE, keys)
                    result_lists = []
                    fetchers = tuple((_cassandra_scalar_multi_load(entity_class, attrs, this_consistency, key_name, these_keys), these_keys) for these_keys in groups)
                    for (fetcher, these_keys) in fetchers:
                        logger.debug('Executing query %s from fetcher %r for keys %s=%r loading attributes %r', fetcher, fetcher, key_name, these_keys, attrs)
                        r = fetcher.fetch_all()

                        #extra diagnostics
                        result_length = len(r)
                        result_keys = frozenset((result[0] for result in r))
                        if len(result_keys) != result_length:
                            logger.warn('Duplicate key in result set %r', r)
                        these_keys_length = len(these_keys)
                        if result_length != these_keys_length:
                            logger.warn('Result set length %d does not match number of keys %d results %r from query %s', len(r), these_keys_length, r, fetcher)
                        for k in these_keys:
                            if k not in result_keys:
                                logger.warn('Could not find key %r in result set group %r as expected from query %s', k, result_keys, fetcher)
                                if not suppress_misses:
                                    raise NullEnvironmentError('%s not found in Cassandra %s' % (k, entity_class.get_primary_column_family()))

                        result_lists.append(r)
                    results = chain(*result_lists)
                    # Results come back in random order so we match them with
                    # their keys.  We make missing results into a None.
                    by_key = {}
                    for result in results:
                        by_key[result[0]] = result
                    results = []
                    for entity in same_attr_entities:
                        key = getattr(entity, key_name)

                        #extra diagnostics
                        if key not in by_key:
                            logger.warn('Could not find key %r in by_key keys %r. Queries: %r', key, by_key.keys(), [str(f) for f in fetchers])

                        result = by_key.get(key)  # Returns None if missing.
                        results.append(result)

                    # Now that we have an aligned list of results, with Nones
                    # for misses, we process them alongside their entities.
                    for result, entity in izip(results, same_attr_entities):
                        if result is None:
                            if suppress_misses:
                                misses.append(entity)
                                continue
                            else:
                                msg = '%s not found in Cassandra %s' % (
                                    entity, entity.get_primary_column_family())
                                raise NullEnvironmentError(msg)
                        else:
                            found_in_scalar_column_family.add(entity)

                        # Promote the results into the session.
                        result_iter = izip(attrs, result)
                        # Pop the key attribute off the iterator.
                        key = result_iter.next()

                        #extra diagnostics
                        if key and (key[1] != entity.key):
                            logger.warn('Entity key %r does not match result key %r for results %r query: %s', entity.key, key, result, fetcher)

                        for attr, encoded_value in result_iter:
                            if encoded_value and attr.related_attribute and not attr.enum:
                                value = attr.cassandra_decode(encoded_value)
                                if session._adjust_if_write_only_connector(entity, attr, value):
                                    # if _adjust_if_write_only_connector
                                    # returns True, the promotion is already
                                    # done.
                                    pass
                                else:
                                    session.promote(entity, attr.name, value)
                            else:
                                # XXX REMOVE THIS HACK AFTER MIGRATING AWAY NONE ENUMS - MEC
                                if encoded_value is None and attr.enum:
                                    value = attr.enum[0]
                                else:
                                    value = attr.cassandra_decode(encoded_value)
                                session.promote(entity, attr.name, value)

            for entity_class_name, by_attrs in by_entity_class_counters.iteritems():
                for attrs, same_attr_entities in by_attrs.iteritems():
                    # Make a list from frozenset 'attrs' for consistent order,
                    # with the key attribute first.
                    key_attr = entity_class.get_key_attribute()
                    key_name = key_attr.name
                    # Make attrs into a list to preserve order.
                    attrs = list(attrs)
                    # Take out the key attribute.
                    try:
                        attrs.remove(key_attr)
                    except ValueError:
                        pass
                    # Insert key attribute at beginning of list.
                    attrs.insert(0, key_attr)

                    # Some convenient local vars we will refer to.
                    entity_class = same_attr_entities[0].entity_class
                    keys = [getattr(e, key_name) for e in same_attr_entities]

                    # Do the multi-load from Cassandra.
                    fetcher = _cassandra_counter_multi_load(
                        entity_class, attrs, this_consistency, key_name, keys)
                    results = fetcher.fetch_all()

                    # Results come back in random order so we match them with
                    # their keys.  We make missing results into a None.
                    by_key = {}
                    for result in results:
                        by_key[result[0]] = result
                    results = tuple((by_key.get(getattr(entity, key_name)) for entity in same_attr_entities))

                    for result, entity in izip(results, same_attr_entities):
                        logger.debug('Processing results %r for entity %r with key %r', result, entity, entity.key)
                        if result is None:
                            entity_fetcher = None
                            if entity not in found_in_scalar_column_family and entity not in has_scalars:
                                # We did not load any scalars, so we don't know if
                                # this object is present there, so let's check
                                # that now.
                                entity_fetcher = _cassandra_scalar_load(
                                    entity, [key_attr], this_consistency,
                                    key_name)
                                result = entity_fetcher.fetch_first()
                                logger.debug('Missing counter row for entity %s so we checkd in scalar cf and got result %r', entity, result)

                                if result:
                                    found_in_scalar_column_family.add(entity)
                                else:
                                    logger.warn('%s not found with scalar query %s after being missed with counter query %s', entity, entity_fetcher, fetcher)

                            if entity in found_in_scalar_column_family:
                                # In this case the counter row was never
                                # initialized.  This is expected in the case of
                                # a migration that adds a count column family.
                                result = [None] * len(attrs)
                                logger.info('%s not found with Cassandra counter query %s but was found with scalar query %s', entity, fetcher, entity_fetcher)
                            else:
                                if suppress_misses:
                                    logger.warn('%s not found with Cassandra counter query %s or scalar query %s', entity, fetcher, entity_fetcher)
                                    misses.append(entity)
                                    continue
                                else:
                                    logger.error('%s not found with Cassandra counter query %s or scalar query %s', entity, fetcher, entity_fetcher)
                                    msg = '%s not found in Cassandra %s or %s' % (
                                        entity,
                                        entity.get_counter_column_family(),
                                        entity.get_primary_column_family())
                                    raise NullEnvironmentError(msg)
                        # Promote the results into the session.
                        result_iter = izip(attrs, result)
                        # Pop the key attribute off the iterator.
                        key = result_iter.next()

                        #extra diagnostics
                        if key and (key[1] != entity.key):
                            if key[1] is None:
                                primary_cf = entity.get_primary_column_family()
                                if entity in found_in_scalar_column_family:
                                    logger.debug('Results for %s came back None because the record was not in %s, but was in scalar family %s, which probably means this record was created prior to a migration which added the counter table', fetcher, entity.get_counter_column_family(), primary_cf)
                                else:
                                    logger.info('Results for %s came back None and the record was not found in scalar_family %s either', fetcher, primary_cf)
                            else:
                                logger.warn('Entity key %r does not match result key %r for results %r query: %s', entity.key, key, result, fetcher)

                        for attr, value in result_iter:
                            # XXX TAKE THIS OUT AFTER COUNTER MIGRATION - MEC
                            if value is None:
                                value = 0
                            # Note we don't bother calling cassandra_decode
                            # here because increments read as None or int,
                            # which is the correct type.
                            session.promote_increment(entity, attr, value)
            for entity in misses:
                # Find the loaders that had these misses, mark them as misses,
                # and add them to the failed_loads list on the loader's
                # top entity.
                for loader in entity_to_loaders[entity]:
                    # If a sub-loader had the issue, the miss belongs to
                    # the base-loader.
                    if isinstance(loader, InventorySubLoader):
                        loader = loader.base_loader
                    loader.load_failed = True
                    # Get the top-level entity for the loader and mark it
                    # failed.
                    top_entity = loader.entity
                    # (this is not efficient algorithmically, but the 
                    # inefficiency should not come to bear.)
                    if loader not in top_entity.failed_loaders:
                        top_entity.failed_loaders.append(loader)
                    top_entity.load_failed = True
                    logger.debug("Load failed for entity: %s", top_entity)

            send_to_list(working_cassandra_shared_loaders,
                         (CASSANDRA_SHARED_LOAD_DONE, frozenset(misses),))

            continue

        # Waiters don't get called until all other business is finished.
        if waiters:
            working_waiters = waiters
            waiters = []
            send_to_list(working_waiters, None)
            continue

def _cassandra_load(column_family, entity, attributes, consistency, key_attr):
    logger.debug('_cassandra_load(column_family=%r, entity=%r, attributes=%r, consistency=%s, key_attr=%r)', column_family, entity, attributes, consistency, key_attr)
    attr_names = [attr.name for attr in attributes]
    fetcher = Fetcher(cf_name=column_family, fields=attr_names, consistency_level=consistency)
    fetcher.add_column_value_relation(key_attr, entity.key)
    logger.debug('Added column value relation for %s to value %r to fetcher %r', key_attr, entity.key, fetcher)
    return fetcher

def _cassandra_multi_load(column_family, entity_class, attributes,
                          consistency, key_attr, keys):
    logger.debug('_cassandra_multi_load(column_family=%r, entity_class=%r, attributes=%r, consistency=%s, key_attr=%r, keys=%r)', column_family, entity_class, attributes, consistency, key_attr, keys)
    attr_names = [attr.name for attr in attributes]
    fetcher = Fetcher(cf_name=column_family, fields=attr_names, consistency_level=consistency)
    fetcher.add_in_relation(key_attr, keys)
    logger.debug('Added in relation for %s to keys %r to fetcher %r', key_attr, keys, fetcher)
    return fetcher

def _cassandra_scalar_load(entity, attributes, consistency, key_attr):
    cf = entity.get_primary_column_family()
    return _cassandra_load(cf, entity, attributes, consistency, key_attr)

def _cassandra_counter_load(entity, attributes, consistency, key_attr):
    cf = entity.get_counter_column_family()
    return _cassandra_load(cf, entity, attributes, consistency, key_attr)

def _cassandra_scalar_multi_load(entity_class, attributes, consistency,
                                 key_attr, keys):
    column_family = entity_class.get_primary_column_family()
    return _cassandra_multi_load(column_family, entity_class, attributes,
                                 consistency, key_attr, keys)

def _cassandra_counter_multi_load(entity_class, attributes, consistency,
                                  key_attr, keys):
    column_family = entity_class.get_counter_column_family()
    return _cassandra_multi_load(column_family, entity_class, attributes,
                                 consistency, key_attr, keys)

@coroutine
def get_lock_coroutine(key, timeout=timedelta(seconds=30)):
    """Get the lock for this key, setting lock data in Redis.

    timeout -- Force the acquisition of lock if it is older than this.

    Returns True if lock acquired, False otherwise.

    The lock identifies this host and this pid.

    """

    # First thing we want is a pipeline, but first thing we will get sent is
    # the arguments for the previous function, which we can safely ignore,
    # as we take no arguments, and were instantiated with the key we are
    # looking for.
    yield

    lock_hsntime = now_hsntime()
    pid = str(os.getpid())
    hostname = settings.NODE_HOSTNAME
    lock_value = dumps([pid, hostname, lock_hsntime])
    pipe = yield PIPELINE_REQUEST
    pipe.setnx(key, lock_value)
    pipe.get(key)
    results_iter = yield RESULTS_REQUEST
    got_lock = results_iter.next()
    lock_data = results_iter.next()
    if not got_lock:
        # Determine if we have to clear a timeout.
        loaded_pid, loaded_hostname, lock_hsntime = loads(lock_data)
        lock_time = from_hsntime(lock_hsntime)
        diff = now() - lock_time
        if diff > timeout:
            # Delete the lock.
            pipe = yield PIPELINE_REQUEST
            pipe.delete(key)
            results_iter = yield RESULTS_REQUEST
            results_iter.next()
            # Log and email the error.
            msg = "manage caches PID %s on %s has been running for %s, removing lock" % (
                loaded_pid, loaded_hostname, stringify(diff))
            logger.warn(msg)
            # Attempt to reacquire.
            yield get_lock_coroutine(key)
    yield True


@coroutine
def release_lock_coroutine(key):
    # call the redis script release_lock with these args, but if it is not
    # ready, you need to refresh it.  If multiple peeps are trying to refresh
    # it, then only one should refresh.

    # first call is args, which will be None
    # TODO: would be cool if we could share pid hostname through args?
    yield

    pid = os.getpid()
    hostname = settings.NODE_HOSTNAME

    # Call the script.
    pipe = yield PIPELINE_REQUEST
    pipe.evalsha(release_lock_script.sha, 3, key, pid, hostname)

    # If we get back an error, request a script init from the loader.
    results_iter = yield RESULTS_REQUEST
    result = results_iter.next()
    if isinstance(result, ResponseError):
        text = result.args[0]
        if text == 'NOSCRIPT No matching script. Please use EVAL.':
            yield SCRIPT_INIT_REQUEST, release_lock_script
            pipe = yield PIPELINE_REQUEST
            pipe.evalsha(release_lock_script.sha, 3, key, pid, hostname)
            # Whether or not there is an error, yield it.
            results_iter = yield RESULTS_REQUEST
            result = results_iter.next()

    yield result

def verify_inventory(entity, inventory_loader):
    """Verify SQL matches the Cassandra inventory loader for this entity."""
    # Get the inventory from SQL
    # Get the inventory from Cassandra
    # Compare the values, print differences.
    #--
    # Get SQL.
    persist = entity.persistent_class.get_by(**{entity.entity_class.key_attribute_name:entity.key})
    # Don't use the active session, use a specific session.
    from backend.environments.memory import _EnvironmentMemory
    mem = _EnvironmentMemory()
    if not persist:
        logger.debug('Could not find %s in postgresql.')
    else:
        mem._promote_inventory_recur(mem, persist, set(), inventory_loader._inventory)

    # Get Cassandra.
    from backend import new_active_session, get_active_session
    new_active_session()
    loader = inventory_loader(entity.key)
    loader.load()

    session = get_active_session()
    # Compare Postgresql to Cassandra
    _diff_mem(mem, session, 'compare postresql to cassandra')
    # Compare Cassandra to Postgresql
    _diff_mem(session, mem, 'compare cassandra to postgresql')

def _diff_mem(m1, m2, name=''):
    missing_classes = []
    missing_keys = []
    missing_attrs = []
    different_values = []
    from backend.datastructures import IncompleteSet
    for entity_class, by_key in m1.values.iteritems():
        by_key2 = m2.values.get(entity_class)
        if by_key2 is None:
            missing_classes.append(entity_class)
        else:
            for key, attrs in by_key.iteritems():
                attrs2 = by_key2.get(key)
                if attrs2 is None:
                    missing_keys.append((entity_class, key))
                else:
                    for attr, v1 in attrs.iteritems():
                        if attr not in attrs2:
                            missing_attrs.append((entity_class, key, attr))
                        else:
                            v2 = attrs2[attr]
#                            if attr == 'thumbnail_aspect':
#                                logger.error('%s %s %s %s %s' % (attr, type(v1), v1, type(v2), v2))
                            if isinstance(v2, IncompleteSet):
                                v2 = v2._contains
                            if isinstance(v1, IncompleteSet):
                                v1 = v1._contains
                            if v1 == '' and v2 is None:
                                pass
                            elif v2 == '' and v1 is None:
                                pass
                            elif isinstance(v1, float) and isinstance(v2, float) and abs(v1-v2) < .00001:
                                pass
                            elif v1 != v2:
                                different_values.append((entity_class, key, attr, v1, v2))
    if missing_classes or missing_keys or missing_attrs or different_values:
        if name:
            logger.debug('%s is missing...', name)
        else:
            logger.debug('missing...')
    if missing_classes:
        logger.debug('classes:')
        for c in missing_classes:
            logger.debug(c.__name__)
    if missing_keys:
        logger.debug('keys')
        for x in missing_keys:
            logger.debug('%s(%s)' % x)
    if missing_attrs:
        logger.debug('attrs')
        for x in missing_attrs:
            logger.debug(x)
    if different_values:
        logger.debug('values')
        for x in different_values:
            logger.debug('%s(%s).%s=%s, got %s' % x)

WRITE_CONSISTENCIES = ['ANY', 'ONE', 'QUORUM', 'LOCAL_QUORUM', 'EACH_QUORUM', 'ALL']
READ_CONSISTENCIES = ['ONE', 'QUORUM', 'LOCAL_QUORUM', 'EACH_QUORUM', 'ALL']
STRICTNESS = {
    'ANY': 0,
    'ONE': 1,
    'QUORUM': 2,
    'LOCAL_QUORUM': 3,
    'EACH_QUORUM': 4,
    'ALL': 5
}
def is_stricter(consistency_a, consistency_b):
    """Returns True if consistency_a is more strict than consistency_b."""
    a_val = STRICTNESS[consistency_a]
    b_val = STRICTNESS[consistency_b]
    return a_val > b_val
