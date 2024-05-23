
"""A cache of the size of a set as defined by a SQL query."""

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


logger = getLogger('hsn.backend.caches.count')


@coroutine
def _count_load(cache):
    # We will be called with an lru_score to kick this off.
    lru_score = yield

    # The main load loop.
    done = False
    while not done:
        # Get data from redis.
        pipe = yield PIPELINE_REQUEST

        # Request the key from the init set.  If the key is there, this
        # cache is initialized.
        key = cache.key_string
        pipe.sismember(cache.get_init_key(), key)

        # Get the count.
        pipe.get(cache.get_count_key())

        # Bump the cache's ranking in the "Least Recently Used" set.
        cache.bump_lru(lru_score, pipe=pipe)

        # yield to get pipeline results
        results = yield RESULTS_REQUEST

        # We got our results from redis, grab our results from the results.
        init_membership = results.next()
        count_str = results.next()
        lru_bump = results.next()

        # If the cache is not inited, request permission to init it.
        if not init_membership:
            # Request permission to initialize this cache.
            permission = yield SQL_PERMISSION_REQUEST
            if permission:
                # Request the init lock.
                got_lock = yield get_lock_coroutine(cache.get_lock_key())
                if got_lock:
                    pipe = yield NO_RESULTS_PIPELINE_REQUEST
                    count = cache.init_from_sql(pipe=pipe)
                    yield NO_RESULTS_REQUEST
                    yield release_lock_coroutine(cache.get_lock_key())
                    done = True
                else:
                    yield WAIT_REQUEST
            else:
                yield WAIT_REQUEST
        else:
            try:
                count = int(count_str)
            except (TypeError, ValueError,):
                logger.exception('Invalid CountCache value %s' % count_str)
                count = 0
            
            done = True

    # Stash the results.
    if cache.load_count_patch:
        setattr(cache.owner_class(cache.key),
                cache.load_count_patch,
                count)
    yield count


class CountLoader(Loader):
    def get_stack_coroutine(self, cache):
        return _count_load(cache)


class CountCache(object):
    # key_base = 'u'
    # owner_class = User
    # entry_class = Post
    # Assign load_count_patch to the string you want to stash count results in
    # load_count_patch = '_post_count'
    load_count_patch = None
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
    def get_init_key(cls):
        return '%si' % cls.key_base

    @classmethod
    def get_lru_key(cls):
        return '%sl' % cls.key_base

    @classmethod
    def get_count_key_prefix(cls):
        return '%sc' % cls.key_base

    def get_count_key(self):
        try:
            return self.count_key
        except AttributeError:
            self.count_key = '%s%s' % (self.get_count_key_prefix(),
                                       self.key_string)
            return self.count_key

    def get_lock_key(self):
        return '%sk%s' % (self.key_base, self.key_string)

    # -- Loaders --------------------------------------------------------------

    def loader(self):
        """Return a loader that loads the size of this cache."""
        return CountLoader(self)

    def load(self):
        """Load the size of this cache."""
        return load(self.loader())

    @classmethod
    def load_multi(cls, owner_keys):
        return load([cls(key).loader() for key in owner_keys])

    # -- Cache Operations -----------------------------------------------------

    @with_redis_pipeline
    def add_init(self, pipe=None):
        pipe.sadd(self.get_init_key(), self.key_string)

    @with_redis_pipeline
    def bump_lru(self, score=None, pipe=None):
        if score is None:
            score = now_hsntime()
        pipe.zadd(self.get_lru_key(), **{self.key_string: score})

    @with_redis_pipeline
    def init_empty(self, lru_score=None, pipe=None):
        self.add_init(pipe=pipe)
        self.set(0, pipe=pipe)
        self.bump_lru(score=lru_score, pipe=pipe)

    @with_redis_pipeline
    def incr(self, value=1, bump=True, pipe=None):
        """Increment.

        bump -- If True, Bump this cache's score in the LRU.
        pipe -- Redis pipeline used by this operation.

        """
        pipe.incr(self.get_count_key(), value)

        if bump:
            self.bump_lru(pipe=pipe)

    @with_redis_pipeline
    def set(self, count, pipe=None):
        pipe.set(self.get_count_key(), count)

    # -- Support Operations (override these) ----------------------------------

    @with_redis_pipeline
    def init_from_sql(self, pipe=None):
        query = self.get_sql_query()

        self.add_init(pipe=pipe)

        count = query.count()
        self.set(count, pipe=pipe)
        self.bump_lru(pipe=pipe)
        return count

    def get_sql_query(self):
        owner_sql_attribute = getattr(self.owner_class.persistent_class,
                         self.owner_class.key_attribute_name)
        entry_sql_attribute = getattr(self.entry_class.persistent_class,
                         self.entry_class.key_attribute_name)
        deleted_sql_attribute = getattr(self.entry_class.persistent_class,
                                        'deleted', None)

        owner_join_sql_attribute = getattr(self.owner_class.persistent_class,
                                           self.owner_join_attribute_name)
        entry_join_sql_attribute = getattr(self.entry_class.persistent_class,
                                           self.entry_join_attribute_name)

        session = backend.core.get_session()
        query = session.query(
                owner_sql_attribute.label('owner_key'),
                entry_sql_attribute.label('entry_key')) \
            .filter(owner_join_sql_attribute==entry_join_sql_attribute) \
            .filter(owner_sql_attribute==self.key)
        if deleted_sql_attribute:
            query = query.filter(deleted_sql_attribute==False)
        return query

    # -- Memory Management ----------------------------------------------------

    def evict(self):
        """Remove this cache from Redis as if it were purged."""
        self.purge([self.key_string])

    @classmethod
    def purge(cls, key_strings):
        """Remove the given set of key-strings from the cache in Redis."""
        lru_key = cls.get_lru_key()
        r = get_redis()
        count_key_template = cls.get_count_key_prefix() + '%s'
        for group in grouper(300, key_strings):
            p = r.pipeline()
            for key_string in group:
                if key_string is None:
                    break
                # Delete the count.
                p.delete(count_key_template % key_string)
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

    def __repr__(self):
        if hasattr(self, 'key'):
            return "%s(%s)" % (self.__class__.__name__,
                               self.key)
        else:
            return self.__class__.__name__

