
"""A full cache of a sorted set.

Can also tell you its count.

"""

from logging import getLogger
from time import sleep
from uuid import UUID

from sqlalchemy.sql import desc

import backend.core
from util.cache import get_redis, with_redis_pipeline
from backend.loader import load, Loader, PIPELINE_REQUEST, \
    RESULTS_REQUEST, NO_RESULTS_PIPELINE_REQUEST, NO_RESULTS_REQUEST, \
    SQL_PERMISSION_REQUEST, WAIT_REQUEST, \
    get_lock_coroutine, release_lock_coroutine
from util.helpers import grouper, coroutine
from util.when import now_hsntime, to_hsntime


logger = getLogger('hsn.backend.caches.fullzset')


@coroutine
def _full_zset_load(cache, start='+inf', end='-inf', count=None,
                    by_scores=False, with_count=False, with_scores=False):
    # with_count = True means return a tuple ([results], total count)
    # Munge the inputs into the format we will use on the redis zset.
    # 'start' and 'end' are either datetimes or ranks
    # depending on 'by_scores'.
    # Groom some inputs.
    if by_scores:
        if '+inf' != start and not isinstance(start, (int, long)):
            raise ValueError('start must be int, was %s' % type(start))
        if '-inf' != end and not isinstance(end, (int, long)):
            raise ValueError('end must be int/long, was %s' % type(end))
    else:
        if '+inf' == start:
            start = 0
        elif not isinstance(start, (int, long)):
            raise ValueError('start must be int/long, was %s' % type(start))
        if '-inf' == end:
            end = -1
        else:
            #TODO: Any integer greater than start should be allowed here
            raise ValueError("if by_scores=False end must be '-inf', was %s" % end)

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

        # Get the count, if we are getting that.
        if with_count:
            pipe.zcard(cache.get_zset_key())

        # Request the items we want.
        args = {'withscores': True}
        if by_scores:
            if count:
                args.update({'num': count, 'start': 0})
            pipe.zrevrangebyscore(cache.get_zset_key(),
                                  start,
                                  end,
                                  **args)
        else:
            # If we aren't requesting by scores we are requesting by rank.
            if count is None:
                request_end = -1
            else:
                # Redis uses an inclusive interpretation of 'end', unlike SQL,
                # so we are sure to request one less than what we want.
                request_end = start + count - 1
            pipe.zrevrange(cache.get_zset_key(),
                           start,
                           request_end,
                           **args)

        # Bump the cache's ranking in the "Least Recently Used" set.
        cache.bump_lru(lru_score, pipe=pipe)

        # Get results from that pipeline.
        results_iter = yield RESULTS_REQUEST

        # We got our results from redis, grab our results from the results.
        init_membership = results_iter.next()
        if with_count:
            results_count = results_iter.next()
        else:
            results_count = None
        zset = results_iter.next()
        lru_bump = results_iter.next()

        # If an initialization is required, do it.
        if not init_membership:
            permission = yield SQL_PERMISSION_REQUEST
            if permission:
                # Request the init lock.
                got_lock = yield get_lock_coroutine(cache.get_lock_key())
                if got_lock:
                    pipe = yield NO_RESULTS_PIPELINE_REQUEST
                    results = cache.init_from_sql(pipe=pipe)
                    results_count = len(results)
                    # Trim the results to the amount requested.
                    if by_scores:
                        if count is not None:
                            results = results[:count + 1]
                    else:
                        if -1 == end:
                            results = results[start:]
                        else:
                            results = results[start:end + 1]
                    # We asked for count + 1, did we get that many?  If so, some items
                    # remain beyond the user's query.
                    if count is None:
                        more = False
                    else:
                        more = (count + 1) == len(results)
                    if more:
                        results = results[:-1]
                    yield NO_RESULTS_REQUEST
                    yield release_lock_coroutine(cache.get_lock_key())
                    done = True
                else:
                    yield WAIT_REQUEST
            else:
                yield WAIT_REQUEST
        else:
            results = [(cache.deserialize_value(p[0]), p[1]) for p in zset]
            done = True

    # if this is a 'with_count' request the results are in a tuple.
    if with_count:
        results = (results, results_count)

    # Groom the results.
    if with_count:
        patch_results = results[0]
    else:
        patch_results = results

    results_with_scores = patch_results
    patch_results = [entry for entry, score in patch_results]
    if with_count:
        results = (patch_results, results[1],)
    else:
        results = patch_results

    # Stash the results.
    if cache.load_patch or cache.load_scores_patch or cache.load_count_patch:
        owner_class = cache.owner_class
        owner = owner_class(cache.key)
        if cache.load_patch:
            patch_attr = owner_class.get_attribute(cache.load_patch)
            if patch_attr:
                # Implement this if we want it, and promote the scores too.
                raise NotImplementedError, "can't promote to an attribute"
            else:
                # If it is a dynamic attribute, we set the value.
                setattr(owner, cache.load_patch, patch_results)
        if cache.load_scores_patch:
            setattr(owner, cache.load_scores_patch, results_with_scores)
        if with_count and cache.load_count_patch:
            setattr(owner, cache.load_count_patch, results[1])

    if with_scores:
        yield results_with_scores
    else:
        yield results


class FullZSetLoader(Loader):
    def get_stack_coroutine(self, *args, **kwargs):
        return _full_zset_load(*args, **kwargs)

@coroutine
def _full_zset_count_load(cache):

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

        # Get the count.
        pipe.zcard(cache.get_zset_key())

        # Bump the cache's ranking in the "Least Recently Used" set.
        cache.bump_lru(lru_score, pipe=pipe)

        # Get results from that pipeline.
        results_iter = yield RESULTS_REQUEST

        # We got our results from redis, grab our results from the results.
        init_membership = results_iter.next()
        count = results_iter.next()
        lru_bump = results_iter.next()

        # If an initialization is required, do it.
        if not init_membership:
            permission = yield SQL_PERMISSION_REQUEST
            if permission:
                # Request the init lock.
                got_lock = yield get_lock_coroutine(cache.get_lock_key())
                if got_lock:
                    pipe = yield NO_RESULTS_PIPELINE_REQUEST
                    results = cache.init_from_sql(pipe=pipe)
                    count = len(results)
                    yield NO_RESULTS_REQUEST
                    yield release_lock_coroutine(cache.get_lock_key())
                    done = True
                else:
                    yield WAIT_REQUEST
            else:
                yield WAIT_REQUEST
        else:
            done = True

    # Stash the results.
    if cache.load_count_patch:
        setattr(cache.owner_class(cache.key),
                cache.load_count_patch,
                count)
    yield count


class FullZSetCountLoader(Loader):
    def get_stack_coroutine(self, cache):
        return _full_zset_count_load(cache)


@coroutine
def _full_zset_load_membership_check(cache, items):
    # We will be called with an lru_score to kick this off.
    lru_score = yield

    if isinstance(items, list):
        result_is_list = True
    else:
        result_is_list = False
        items = [items]

    # The main load loop.
    done = False
    while not done:
        # This is the first state, needing to get data from redis.
        pipe = yield PIPELINE_REQUEST

        # Request the key from the init set.  If the key is there, this
        # cache is initialized.
        key_str = cache.key_string
        pipe.sismember(cache.get_init_key(), key_str)

        # Make the membership tests.
        for i in items:
            pipe.zscore(cache.get_zset_key(), cache.serialize_value(i))

        # Bump the cache's ranking in the "Least Recently Used" set.
        cache.bump_lru(lru_score, pipe=pipe)

        results_iter = yield RESULTS_REQUEST

        # We got our results from redis, grab our results from the results.
        init_membership = results_iter.next()
        # A list of bools for the corresponding entries.
        memberships = [bool(results_iter.next()) for i in items]
        lru_bump = results_iter.next()

        init_required = True
        try:
            memberships.index(False)
        except ValueError:
            # In this case we found everything, so we are done.
            init_required = False
        else:
            if init_membership:
                # In this case the cache was initialized, so whatever results
                # we have are valid.
                init_required = False

        if init_required:
            # In this case we need to get permission to initialize this
            # cache, and then initialize it.
            permission = yield SQL_PERMISSION_REQUEST
            if permission:
                # Request the init lock.
                got_lock = yield get_lock_coroutine(cache.get_lock_key())
                if got_lock:
                    pipe = yield NO_RESULTS_PIPELINE_REQUEST
                    results = cache.init_from_sql(pipe=pipe)
                    member_keys = set([item.key for item, score in results])
                    memberships = [m.key in member_keys for m in items]
                    yield NO_RESULTS_REQUEST
                    yield release_lock_coroutine(cache.get_lock_key())
                    done = True
                else:
                    yield WAIT_REQUEST
            else:
                yield WAIT_REQUEST
        else:
            done = True

    if result_is_list:
        result = memberships
    else:
        result = memberships[0]

    yield result


class FullZSetMembershipLoader(Loader):
    def get_stack_coroutine(self, cache, items):
        return _full_zset_load_membership_check(cache, items)

    def get_session_hash(self):
        """Returns a hashable for the session to test Loader equivalence."""
        cache_class = type(self.args[0]).__name__
        items = self.args[1]
        if not isinstance(items, list):
            items = [items]
        items = frozenset(items)
        if self.kwargs:
            kwargs = frozenset(self.kwargs.items())
            return (cache_class, items, kwargs)
        else:
            return (cache_class, items)


class FullZSetCache(object):
    # key_base = 'u'
    # owner_class = User
    # entry_class = Post
    # score_attribute_name = 'created_on'
    # Assign load_patch to the string you want to stash the load result in
    # ex: Post._reposters gets the result of a load, load_patch = '_reposters'
    load_patch = None
    # Assign load_count_patch to the string you want to stash count results in
    load_count_patch = None
    load_scores_patch = None
    is_score_datetime = False

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
    def get_zset_key_prefix(cls):
        return '%sz' % cls.key_base

    def get_zset_key(self):
        try:
            return self.zset_key
        except AttributeError:
            self.zset_key = '%s%s' % (self.get_zset_key_prefix(),
                                       self.key_string)
            return self.zset_key

    def get_lock_key(self):
        return '%sk%s' % (self.key_base, self.key_string)

    # -- Loaders --------------------------------------------------------------

    def loader(self, *args, **kwargs):
        """Return a loader that loads the elements of this cache."""
        return FullZSetLoader(self, *args, **kwargs)

    def count_loader(self):
        """Return a loader that loads the size of this cache."""
        return FullZSetCountLoader(self)

    def membership_check_loader(self, entry):
        return FullZSetMembershipLoader(self, entry)

    def load(self, start='+inf', end='-inf', count=None, by_scores=False,
             with_scores=False):
        """Load entries from redis Cache, initing from SQL as needed.

        Returns a list of (Entry, score) tuples.

        start -- Return entries with score/rank no greater than this.
        end -- Return entries with score/rank greater than this.
        count -- Return no more than this many items.
        by_scores -- True if initing by score, False if initing by rank.
        with_scores -- If True, results are in (entry, score) format.

        """
        return load(self.loader(start=start, end=end, count=count,
                                by_scores=by_scores, with_scores=with_scores))

    @classmethod
    def load_multi(cls, keys, *args, **kwargs):
        return load([cls(key).loader(*args, **kwargs) for key in keys])

    def load_count(self):
        return load(self.count_loader())

    @classmethod
    def load_count_multi(cls, owner_keys):
        return load([cls(key).count_loader() for key in owner_keys])

    def load_membership_check(self, item):
        """returns True if item is a member.

        item -- item or items to test for membership.

        If input is list of items then results is list of membership results.

        """
        return load(self.membership_check_loader(item))

    def load_membership_check_multi(self, items):
        return load([self.membership_check_loader(item) for item in items])

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
        self.add_none_remain_marker(pipe=pipe)
        self.bump_lru(score=lru_score, pipe=pipe)

    @with_redis_pipeline
    def add(self, input, score=None, bump=True, pipe=None):
        """Add a record to the set.

        input -- Input to be passed to serialize_value and stored in zset.
        score -- ZSet score, if None call 'cls.score_attribute_name' on input.
        bump -- If True, Bump this cache's score in the LRU.
        pipe -- Redis pipeline used by this operation.

        """
        if input.write_only:
            raise ValueError, 'write_only input not allowed.'

        if score is None:
            score = getattr(input, self.score_attribute_name)
            if self.is_score_datetime:
                score = to_hsntime(score)
        pipe.zadd(self.get_zset_key(), **{self.serialize_value(input): score})
        if bump:
            self.bump_lru(pipe=pipe)

    @with_redis_pipeline
    def bump(self, input, score=None, bump=True, pipe=None):
        """Change the score of an existing entry.
        
        input -- Input to be passed to serialize_value and changed in zset.
        score -- ZSet score, if None call 'cls.score_attribute_name' on input.
        bump -- If True, Bump this cache's score in the LRU.
        pipe -- Redis pipeline used by this operation.

        """
        temp_key = '_c'
        zset_key = self.get_zset_key()
        if score is None:
            score = getattr(input, self.score_attribute_name)
        pipe.delete(temp_key)
        pipe.zadd(temp_key, **{self.serialize_value(input): score})
        # Merge temp zset into our zset, combine scores with a 'MAX' function.
        pipe.zunionstore(zset_key, [zset_key, temp_key], aggregate='MAX')
        if bump:
            self.bump_lru(pipe=pipe)

    @with_redis_pipeline
    def remove_multi(self, items, bump=True, pipe=None):
        items_data = [self.serialize_value(i) for i in items]
        pipe.zrem(self.get_zset_key(), *items_data)
        if bump:
            self.bump_lru(pipe=pipe)

    @with_redis_pipeline
    def remove(self, item, bump=True, pipe=None):
        pipe.zrem(self.get_zset_key(), self.serialize_value(item))
        if bump:
            self.bump_lru(pipe=pipe)

    # -- Support Operations (override these) ----------------------------------

    @classmethod
    def serialize_value(cls, entry):
        """Serialize entry data for storage in the redis zset.

        This must be a stable function as it is a key in the zset.

        """
        k = entry.key
        try:
            return k.bytes
        except AttributeError:
            return str(k)

    @classmethod
    def deserialize_value(cls, data):
        """Deserialize data from redis zset, see 'serialize_value'."""
        value_class = cls.entry_class.get_key_attribute().value_class
        if value_class == UUID:
            key = UUID(bytes=data)
        else:
            key = value_class(data)
        return cls.entry_class(key)

    @classmethod
    def get_score_persistent_attribute(cls):
        return getattr(cls.entry_class.persistent_class,
                       cls.score_attribute_name)

    @classmethod
    def persistent_to_entry(cls, record):
        """Given a persistent query result, make an entry.

        This base-case version looks for an attributes named 'entry_key' and
        'score'.

        If this is sufficient for your cache, have your query return an entry
        with the proper attribute names.  Or override this with whatever you
        need to encode this into the cache.

        This must return the same results as 'deserialize_value', though
        given the input from a persistent query (instead of data read-back from
        the cache as deserialize_value does.)

        """
        entry_key = getattr(record, 'entry_key')
        entity = cls.entry_class(entry_key)

        if cls.is_score_datetime:
            score = to_hsntime(getattr(record, 'score'))
        else:
            score = getattr(record, 'score')
        return (entity, score,)

    def get_sql_query(self):
        owner_sql_attribute = getattr(self.owner_class.persistent_class,
                         self.owner_class.key_attribute_name)
        entry_sql_attribute = getattr(self.entry_class.persistent_class,
                         self.entry_class.key_attribute_name)
        score_sql_attribute = self.get_score_persistent_attribute()
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
                score_sql_attribute.label('score')) \
            .filter(owner_join_sql_attribute==entry_join_sql_attribute) \
            .filter(owner_sql_attribute==self.key)
        if deleted_sql_attribute:
            query = query.filter(deleted_sql_attribute==False)
        return query

    @with_redis_pipeline
    def init_from_sql(self, pipe=None):
        """Use SQL to initialize a zset cache in Redis.

        Returns a list of (Entry, score) tuples.

        start -- Return entries with score/rank no greater than this.
        end -- Return entries with score/rank greater than this.
        count -- Return no more than this many items.
        by_scores -- True if initing by score, False if initing by rank.
        with_count -- Return the total count of the set in a tuple with result.
        pipe -- The Redis pipeline to which we write the initialization.

        """
        # Get the data requested from SQL.
        query = self.get_sql_query()

        self.add_init(pipe=pipe)

        score_attr = self.get_score_persistent_attribute()

        # The query must be in order.
        query = query.order_by(desc(score_attr))

        # Promote the SQL data to backend Entities.
        entries = [self.persistent_to_entry(p) for p in query.all()]

        # Add the items to Redis.
        for entry, score in entries:
            self.add(entry, score, pipe=pipe)

        return entries

    # -- Memory Management ----------------------------------------------------

    def evict(self):
        """Remove this cache from Redis as if it were purged."""
        self.purge([self.key_string])

    @classmethod
    def purge(cls, key_strings):
        """Remove the given set of key-strings from the cache in Redis."""
        lru_key = cls.get_lru_key()
        r = get_redis()
        zset_key_template = cls.get_zset_key_prefix() + '%s'
        for group in grouper(300, key_strings):
            p = r.pipeline()
            for key_string in group:
                if key_string is None:
                    break
                # Delete the ZSet.
                p.delete(zset_key_template % key_string)
                # Remove from the init set.
                p.srem(cls.get_init_key(), key_string)
                # Remove from the LRU zset.
                p.zrem(lru_key, key_string)
                # This is so PostCache can also cull some helper structures.
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
