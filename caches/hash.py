
"""A cache of a mapping of the owner's entries to entry-specific values."""
from itertools import izip
from logging import getLogger
from time import sleep
from uuid import UUID

import backend.core
from util.cache import get_redis, with_redis_pipeline
from backend.loader import load, Loader, PIPELINE_REQUEST, \
    RESULTS_REQUEST, NO_RESULTS_PIPELINE_REQUEST, NO_RESULTS_REQUEST, \
    SQL_PERMISSION_REQUEST, WAIT_REQUEST, \
    get_lock_coroutine, release_lock_coroutine
from util.helpers import grouper, coroutine
from util.when import now_hsntime


logger = getLogger('hsn.backend.caches.hash')
REMAINING_PREFIX = '/R'


@coroutine
def _hash_load(cache):
    # First yield is lru_score
    lru_score = yield

    # The main load loop.
    done = False
    while not done:
        # Get a shared Redis pipeline.
        pipe = yield PIPELINE_REQUEST

        # Request the key from the init set.  If the key is there, this
        # cache is initialized.
        key = cache.key_string
        pipe.sismember(cache.get_init_key(), key)

        # Get the remaining marker, which is last item in a valid set.
        pipe.hgetall(cache.get_hash_key())

        # Bump the cache's ranking in the "Least Recently Used" set.
        cache.bump_lru(lru_score, pipe=pipe)

        # Get results from that pipeline.
        results_iter = yield RESULTS_REQUEST

        # We got our results from redis, grab our results from the results.
        init_membership = results_iter.next()
        contents = results_iter.next()
        lru_bump = results_iter.next()

        if not init_membership:
            permission = yield SQL_PERMISSION_REQUEST
            if permission:
                # Request the init lock.
                got_lock = yield get_lock_coroutine(cache.get_lock_key())
                if got_lock:
                    pipe = yield NO_RESULTS_PIPELINE_REQUEST
                    serialized_results = cache.init_from_sql(pipe=pipe)
                    # results is (serialized key, serialized value), but
                    # we want to return deserialized versions of this data.
                    yield NO_RESULTS_REQUEST
                    yield release_lock_coroutine(cache.get_lock_key())
                    results = {}
                    for k, v in serialized_results.iteritems():
                        key = cache.deserialize_key(k)
                        value = v if v is None else cache.deserialize_value(v)
                        results[key] = value
                    done = True
                else:
                    yield WAIT_REQUEST
            else:
                yield WAIT_REQUEST
        else:
            results = dict(
                [(cache.deserialize_key(p[0]),
                  None if p[1] is None else cache.deserialize_value(p[1]),)
                  for p in contents.iteritems()])
            done = True

    # Stash the results.
    if cache.load_patch:
        owner_class = cache.owner_class
        entity_attr = owner_class.get_attribute(cache.load_patch)
        if entity_attr:
            # If the patch string was an actual attribute, promote.
            raise NotImplementedError, 'HashCache needs a promote'
        else:
            # If it is a dynamic attribute, we set the value.
            setattr(cache.owner_class(cache.key),
                    cache.load_patch,
                    results)
    yield results


class HashLoader(Loader):
    def get_stack_coroutine(self, *args, **kwargs):
        return _hash_load(*args, **kwargs)

    def get_session_hash(self):
        """Returns a hashable for the session to test Loader equivalence."""
        cache_class = type(self.args[0]).__name__
        return cache_class


@coroutine
def _hash_load_value(cache, items):
    # We will be called with an lru_score to kick this off.
    lru_score = yield

    # The main load loop's control flag.
    done = False
    # The result we will return goes here.
    result = {}

    # Check for no-ops.
    if items is None:
        done = True
    elif isinstance(items, (list, set, frozenset)) and len(items) == 0:
        done = True
#    if not items:
#        result =
#        raise ValueError, \
#            u"'items' must be an entry or a list of entries, was (%s)'%s'" % (
#                type(items), items)
    elif not isinstance(items, list):
        items = [items]

    while not done:
        # This is the first state, needing to get data from redis.
        pipe = yield PIPELINE_REQUEST

        key_str = cache.key_string
        # Request the key from the init set.  If the key is there, this
        # cache is initialized.
        pipe.sismember(cache.get_init_key(), key_str)

        # Make the membership tests.
        pipe.hmget(cache.get_hash_key(),
                   [cache.serialize_key(i) for i in items])

        # Bump the cache's ranking in the "Least Recently Used" set.
        cache.bump_lru(lru_score, pipe=pipe)

        results_iter = yield RESULTS_REQUEST

        # Get our results from the results iterator.
        init_membership = results_iter.next()
        values = results_iter.next()
        lru_bump = results_iter.next()

        if not init_membership:
            permission = yield SQL_PERMISSION_REQUEST
            if permission:
                # Request the init lock.
                got_lock = yield get_lock_coroutine(cache.get_lock_key())
                if got_lock:
                    pipe = yield NO_RESULTS_PIPELINE_REQUEST
                    serialized_results = cache.init_from_sql(items, pipe=pipe)
                    yield NO_RESULTS_REQUEST
                    yield release_lock_coroutine(cache.get_lock_key())
                    result = {}
                    for i in items:
                        try:
                            v = serialized_results[cache.serialize_key(i)]
                            result[i]=cache.deserialize_value(v) if v else None
                        except KeyError:
                            result[i]=None
                    done = True
                else:
                    yield WAIT_REQUEST
            else:
                yield WAIT_REQUEST
        else:
            result = dict(
                [(item, None if v is None else cache.deserialize_value(v),) for item, v in izip(items, values)])
            done = True

    if cache.load_patch:
        owner = cache.owner_class(cache.key)
        try:
            patch_to = getattr(owner, cache.load_patch)
            patch_to.update(result)
        except AttributeError:
            setattr(owner, cache.load_patch, result)

    yield result


class HashValueLoader(Loader):
    def get_stack_coroutine(self, cache, items):
        return _hash_load_value(cache, items)

    def get_session_hash(self):
        """Returns a hashable for the session to test Loader equivalence."""
        cache_class = type(self.args[0]).__name__
        items = self.args[1]
        if not isinstance(items, list):
            items = [items]
        items = frozenset(items)
        return (cache_class, items)


class HashCache(object):
    # key_base = 'u'
    # owner_class = Post
    # entry_class = User
    # value_attribute_name = 'mood'
    # Assign load_patch to the string you want to stash the load result in.
    # ex: Post._reposters gets the result of a load, load_patch = '_reposters'
    # A HashCache merges results in with existing results, if any.
    load_patch = None
    # owner_join_attribute_name = 'uuid'
    # entry_join_attribute_name = 'user_uuid'

    # -- Init -----------------------------------------------------------------

    def __init__(self, key):
        self.key = key
        # For when this cache's key appears in Redis.
        if isinstance(key, UUID):
            self.key_string = key.bytes
        else:
            self.key_string = str(key)

    # -- Keys -----------------------------------------------------------------

    @classmethod
    def get_lru_key(cls):
        return '%sl' % cls.key_base

    @classmethod
    def get_init_key(cls):
        return '%si' % cls.key_base

    @classmethod
    def get_hash_key_prefix(cls):
        return '%sh' % cls.key_base

    def get_hash_key(self):
        try:
            return self.hash_key
        except AttributeError:
            self.hash_key = '%s%s' % (self.get_hash_key_prefix(),
                                      self.key_string)
            return self.hash_key

    def get_lock_key(self):
        return '%sk%s' % (self.key_base, self.key_string)

    # -- Loaders --------------------------------------------------------------

    def loader(self):
        """Return a loader that loads the key value pairs of this cache."""
        return HashLoader(self)

    def load(self):
        """Load entries from redis Cache, initing from SQL as needed.

        Returns a list of (Entry, value) tuples.

        """
        loader = HashLoader(self)
        return load(loader)

    @classmethod
    def load_multi(cls, keys):
        loaders = [HashLoader(cls(key)) for key in keys]
        return load(loaders)

    def value_loader(self, item):
        """Return a lodaer that loads the value for a given hash key(s)."""
        return HashValueLoader(self, item)

    def load_value(self, item):
        """returns the value for item if it is a member, else None.

        item -- item or items to get.

        If input is list of items then results is list of value results.

        """
        return load(self.value_loader(item))

    @classmethod
    def load_value_multi(cls, owner_keys, item):
        return load([cls(k).value_loader(item) for k in owner_keys])

    # -- Cache Operations -----------------------------------------------------

    @with_redis_pipeline
    def bump_lru(self, score=None, pipe=None):
        if score is None:
            score = now_hsntime()
        pipe.zadd(self.get_lru_key(), **{self.key_string: score})

    @with_redis_pipeline
    def add_init(self, pipe=None):
        pipe.sadd(self.get_init_key(), self.key_string)

    @with_redis_pipeline
    def check_init(self, pipe=None):
        pipe.sismember(self.get_init_key(), self.key_string)

    @with_redis_pipeline
    def init_empty(self, lru_score=None, pipe=None):
        self.add_init(pipe=pipe)
        self.bump_lru(score=lru_score, pipe=pipe)

    @with_redis_pipeline
    def add(self, input, value=None, bump=True, pipe=None):
        """Add a record to the set.

        input -- Input to be passed to serialize_value and made into a key.
        value -- encoded value, if None call serialize_value on input.
        bump -- If True, Bump this cache's score in the LRU.
        pipe -- Redis pipeline used by this operation.

        """
        if value is None:
            value = self.serialize_value(input)
        pipe.hset(self.get_hash_key(), self.serialize_key(input), value)
        if bump:
            self.bump_lru(pipe=pipe)

    @with_redis_pipeline
    def remove_multi(self, items, bump=True, pipe=None):
        items_data = [self.serialize_key(i) for i in items]
        pipe.hdel(self.get_hash_key(), *items_data)
        if bump:
            self.bump_lru(pipe=pipe)

    @with_redis_pipeline
    def remove(self, item, bump=True, pipe=None):
        pipe.hdel(self.get_hash_key(), self.serialize_key(item))
        if bump:
            self.bump_lru(pipe=pipe)

    # -- Support Operations (override these) ----------------------------------

    @classmethod
    def get_value_persistent_attribute(cls):
        return getattr(cls.entry_class.persistent_class,
                       cls.value_attribute_name)

    @classmethod
    def serialize_key(cls, entry):
        """Serialize entry key for use in the redis hash.

        This must be a stable function as it is a key in the hash.

        """
        k = entry.key
        try:
            return k.bytes
        except AttributeError:
            return str(k)        

    @classmethod
    def serialize_value(cls, entry):
        """Serialize entry data for storage in the redis hash.

        This must be a stable function as it is a key in the zset.

        """
        k = entry.key
        try:
            return k.bytes
        except AttributeError:
            return str(k)

    @classmethod
    def deserialize_key(cls, data):
        """Deserialize key from redis hash, see 'serialize_key'."""
        value_class = cls.entry_class.get_key_attribute().value_class
        if value_class == UUID:
            key = UUID(bytes=data)
        else:
            key = value_class(data)
        return cls.entry_class(key)

    @classmethod
    def deserialize_value(cls, data):
        """Deserialize data from redis zset, see 'serialize_value'."""
        value_class = cls.entry_class.get_attribute(cls.value_attribute_name).value_class
        if value_class == UUID:
            key = UUID(bytes=data)
        else:
            key = value_class(data)
        return cls.entry_class(key)

    @classmethod
    def persistent_to_entry(cls, record):
        """Given a persistent query result, make an entry."""
        # TODO: Why do we use 'getattr' here?
        entry_key = getattr(record, 'entry_key')
        entry = cls.entry_class(entry_key)
        value = getattr(record, 'value')
        return (entry, value,)

    def get_sql_query(self):
        owner_sql_attribute = getattr(self.owner_class.persistent_class,
                         self.owner_class.key_attribute_name)
        entry_sql_attribute = getattr(self.entry_class.persistent_class,
                         self.entry_class.key_attribute_name)
        value_sql_attribute = self.get_value_persistent_attribute()
        deleted_sql_attribute = getattr(self.entry_class.persistent_class,
                                        'deleted', None)

        owner_join_sql_attribute = getattr(self.owner_class.persistent_class,
                                           self.owner_join_attribute_name)
        entry_join_sql_attribute = getattr(self.entry_class.persistent_class,
                                           self.entry_join_attribute_name)

        session = backend.core.get_session()
        query = session.query(
                owner_sql_attribute.label('owner_key'),
                entry_sql_attribute.label('entry_key'),
                value_sql_attribute.label('value')) \
            .filter(owner_join_sql_attribute==entry_join_sql_attribute) \
            .filter(owner_sql_attribute==self.key)
        if deleted_sql_attribute:
            query = query.filter(deleted_sql_attribute==False)
        return query

    # Note that the HashCaches have implemented their own init which would
    # be reducable here if we had serialize_persistent_key,value functions.
    @with_redis_pipeline
    def init_from_sql(self, items=None, pipe=None):
        """Use SQL to initialize a HashCache in Redis.

        Returns a dict of (serialized key, serialized value) tuples.

        items -- The entire set is initialized, only items results return.
        pipe -- The Redis pipeline to which we write the initialization.

        """
        # Get the data requested from SQL.
        persistent_items = self.get_sql_query().all()

        # Promote the SQL data to backend Entities.
        entries = [self.persistent_to_entry(p) for p in persistent_items]

        # We do this here to prevent doing it twice.
        serialized_keys = [self.serialize_key(e) for e, v in entries]

        entry_dict = {}
        for key, (entry, value) in izip(serialized_keys, entries):
            entry_dict[key] = self.serialize_value(value)

        # Add the items to Redis.
        self.add_init(pipe=pipe)
        if entry_dict:
            pipe.hmset(self.get_hash_key(), entry_dict)
        self.bump_lru(pipe=pipe)

        if items is None:
            result = entry_dict
        else:
            result = {}
            for key in serialized_keys:
                try:
                    result[key] = entry_dict[key]
                except KeyError:
                    result[key] = None

        return result

    # -- Memory Management ----------------------------------------------------

    def evict(self):
        """Remove this cache from Redis as if it were purged."""
        self.purge([self.key_string])

    @classmethod
    def purge(cls, key_strings):
        """Remove the given set of key-strings from the cache in Redis."""
        lru_key = cls.get_lru_key()
        r = get_redis()
        key_template = cls.key_base + '%s'
        for group in grouper(300, key_strings):
            p = r.pipeline()
            for key_string in group:
                if key_string is None:
                    break
                # Delete the Hash.
                p.delete(key_template % key_string)
                # Remove from the init set.
                p.srem(cls.get_init_key(), key_string)
                # Remove from the LRU zset.
                p.zrem(lru_key, key_string)
                # This is so a Cache can also cull some helper structures.
                cls._special_purge(key_string, p)
            p.execute()
            # Sleep to prevent redis hosage.
            sleep(.1)

    @classmethod
    def _special_purge(cls, key_string, pipeline):
        """This is here so a Cache can cull some helper structures."""
        pass

    # -- Misc -----------------------------------------------------------------

    def render_remaining_marker(self):
        return '%s%s' % (REMAINING_PREFIX, self.key.bytes)

    def __repr__(self):
        if hasattr(self, 'key'):
            return "%s(%s)" % (self.__class__.__name__,
                               self.key)
        else:
            return self.__class__.__name__
