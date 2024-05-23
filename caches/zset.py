
"""A partial cache of a sorted set.

Must be accessed and initialized from top-to-bottom.  It is inefficient to
attempt to access this set in the middle if it has not been initialized
because it must initialize from the beginning up to the given point.

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
from util.when import now_hsntime, from_hsntime, to_hsntime


logger = getLogger('hsn.backend.caches.zset')
REMAINING_PREFIX = '/R'


@coroutine
def _zset_load(cache, start='+inf', end='-inf', count=None,
               by_scores=False, cache_size=None, with_scores=False):
    # Munge the inputs into the format we will use on the redis zset.
    # 'start' and 'end' are either scores or ranks depending on 'by_scores'.
    if by_scores:
        if '+inf' != start and not isinstance(start, (int, long)):
            raise ValueError('start must be int/long, was %s' % type(start))
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

        # Get the remaining marker, which is last item in a valid set.
        pipe.zrange(cache.get_zset_key(), 0, 1, withscores=True)

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
        remaining_range = results_iter.next()
        zset = results_iter.next()
        lru_bump = results_iter.next()

        # We will set this flag to False if an init is not needed.
        init_required = True

        # We use a try block to determine if we have valid results
        # or need to init.
        try:
            # If the key is not in the init set we need to init.
            if not init_membership:
                raise Exception

            # I don't expect the remaining marker to not be there, but if
            # it is not, then this set is not initialized.
            if not remaining_range:
                raise Exception

            # See what we have for a remaining marker.
            remaining_tuple = remaining_range[0]
            remaining_marker, remaining_score = remaining_tuple

            if not remaining_marker.startswith(REMAINING_PREFIX):
                raise Exception

            # Do we have enough results to return or do we need to init?
            entries = [r for r in zset if r != remaining_tuple]
            # In all cases, if remaining score is -inf, we have a full
            # zset and can return what we found.
            if float('-inf') == remaining_score:
                init_required = False
            elif by_scores:
                # Did we seek a limited amount, or a range of items?
                if count is None:
                    # Not a range, but was it limited?
                    if remaining_score == end or \
                       (isinstance(end, int) and \
                       end <= remaining_score):
                        # There were enough items to satisfy the request.
                        init_required = False
                    else:
                        # We hit the remaining marker before we got the
                        # range we were looking for, so this is a miss.
                        pass
                else:
                    # A count was given, do we have that many items?
                    if len(entries) >= count:
                        init_required = False
            else:
                # We are requesting by rank, not scores.
                if count is None:
                    if end is None:
                        # We want all times from a certain rank onwards,
                        # but the set had remaining items in SQL, so it
                        # is insufficient.
                        pass
                    else:
                        # Did we get all the items we wanted?
                        if len(entries) >= (end - start):
                            init_required = False
                else:
                    # A count was given, do we have enough items?
                    if len(entries) >= count:
                        init_required = False

        except Exception:
            pass

        if init_required:
            permission = yield SQL_PERMISSION_REQUEST
            if permission:
                # Request the init lock.
                got_lock = yield get_lock_coroutine(cache.get_lock_key())
                if got_lock:
                    pipe = yield NO_RESULTS_PIPELINE_REQUEST
                    if by_scores:
                        request_end = end
                    elif count is None:
                        request_end = -1
                    else:
                        request_end = start + count
                    results = cache.init_from_sql(
                        start, request_end, count, by_scores, cache_size,
                        pipe=pipe)
                    yield NO_RESULTS_REQUEST
                    yield release_lock_coroutine(cache.get_lock_key())
                    done = True
                else:
                    yield WAIT_REQUEST
            else:
                yield WAIT_REQUEST
        else:
            results = [(cache.deserialize_value(p[0]), p[1]) for p in entries]
            done = True

    # Filter out the scores.
    results_with_scores = results
    results = [entry for entry, score in results_with_scores]

    if cache.load_patch or cache.load_scores_patch:
        owner_class = cache.owner_class
        owner = owner_class(cache.key)
        if cache.load_patch:
            patch_attr = owner_class.get_attribute(cache.load_patch)
            if patch_attr:
                # Implement this if we want it, and promote the scores too.
                raise NotImplementedError, "can't promote to an attribute"
            else:
                # If it is a dynamic attribute, we set the value.
                setattr(owner, cache.load_patch, results)
        if cache.load_scores_patch:
            setattr(owner, cache.load_scores_patch, results_with_scores)

    # If scores were requested, return them.
    if with_scores:
        yield results_with_scores
    else:
        yield results


class ZSetLoader(Loader):
    def get_stack_coroutine(self, *args, **kwargs):
        return _zset_load(*args, **kwargs)


@coroutine
def _zset_load_membership_check(cache, items):
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


class ZSetMembershipLoader(Loader):
    def get_stack_coroutine(self, cache, items):
        return _zset_load_membership_check(cache, items)

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


class ZSetCache(object):
    # key_base = 'u'
    # owner_class = User
    # entry_class = Post
    # score_attribute_name = 'created_on'
    # Assign load_patch to the string you want to stash the load result in.
    # If it is an attribute the value will be 'promoted'.
    # ex: Post._reposters gets the result of a load, load_patch = '_reposters'
    load_patch = None
    # entry, score tuples will be placed in this dynamic attribute.
    load_scores_patch = None
    # owner_join_attribute_name = 'uuid'
    # entry_join_attribute_name = 'user_uuid'
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

    @classmethod
    def derive_count_cache_declaration(cls, key_base, cache_name=None, docstring=None):
        """Make a CountCache whose definition is derived from this zset's definition. If cache_name is not provided, it will attempt to derive a name based on the zset's name.
        If you want a CountCache that tracks the total entities in a zset, this will handily create one for you. Example use case:
        from logic.purchases.cache.purchases import ProjectPurchasesCache
        ProjectPurchasesCountCache = ProjectPurchasesCache.derived_count_cache_declaration()
        """
        from backend.caches.count import CountCache
        copy_attributes = ('owner_class', 'entry_class', 'score_attribute_name')
        attribute_dictionary = dict(((attribute, getattr(cls, attribute)) for attribute in copy_attributes if hasattr(cls, attribute)))
        attribute_dictionary['key_base'] = key_base
        attribute_dictionary['get_sql_query'] = cls.__dict__['get_sql_query']
        if docstring is not None:
            attribute_dictionary['__doc__'] = docstring
        else:
            attribute_dictionary['__doc__'] = 'CountCache derived from %s: %s' % (cls.__name__, __doc__)

        if cache_name is None and cls.__name__.endswith('Cache'):
            #If our name ends in Cache, the derived class should be the same but ending with CountCache
            cache_name = cls.__name__[:-5] + 'CountCache'

        attribute_dictionary['load_count_patch'] = cls.load_patch + "_count"
        return type(cache_name, (CountCache,), attribute_dictionary)

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
        return ZSetLoader(self, *args, **kwargs)

    def load(self, start='+inf', end='-inf', count=None, by_scores=False,
             cache_size=None, with_scores=False):
        """Load entries from redis Cache, initing from SQL as needed.

        Returns a list of (Entry, score) tuples.

        start -- Return entries with score/rank no greater than this.
        end -- Return entries with score/rank greater than this.
        count -- Return no more than this many items.
        by_scores -- True if initing by score, False if initing by rank.
        cache_size -- Initialize the cache with this many items.
        with_scores -- If True, results are in (entry, score) format.

        """
        loader = ZSetLoader(self, start=start, end=end, count=count,
                            by_scores=by_scores, cache_size=cache_size,
                            with_scores=with_scores)
        return load(loader)

    @classmethod
    def load_multi(cls, keys, *args, **kwargs):
        loaders = [cls(key).loader(*args, **kwargs) for key in keys]
        return load(loaders)

    def membership_check_loader(self, item):
        return ZSetMembershipLoader(self, item)

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
        """Update the score of an existing entry.  (increases the score only.)
        
        input -- Input to be passed to serialize_value and changed in zset.
        score -- ZSet score, if None call 'cls.score_attribute_name' on input.
        bump -- If True, Bump this cache's score in the LRU.
        pipe -- Redis pipeline used by this operation.

        """
        # Note: I assume this is not as quick as just calling zadd on the zset,
        # so I didn't force 'add' to use this same "increase-only"
        # implementation.
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

    @with_redis_pipeline
    def add_remaining_marker(self, score, pipe=None):
        pipe.zadd(self.get_zset_key(), **{self.render_remaining_marker(): score})

    @with_redis_pipeline
    def add_none_remain_marker(self, pipe=None):
        self.add_remaining_marker('-inf', pipe=pipe)

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
            try:
                key = UUID(bytes=data)
            except ValueError, e:
                logger.error('Got ValueError %s attempting to make a UUID out of bytes "%s", type %s, substituting None' % (e, data, type(data)))
                return None
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
        # TODO: Why do we use 'getattr' here?
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
    def init_from_sql(self, start='+inf', end='-inf', count=None,
                      by_scores=False, cache=None, pipe=None):
        """Use SQL to initialize a zset cache in Redis.

        Returns a list of (Entry, score) tuples.

        start -- Return entries with score/rank no greater than this.
        end -- Return entries with score/rank greater than this.
        count -- Return no more than this many items.
        by_scores -- True if initing by score, False if initing by rank.
        cache -- Initialize the cache with this many items.
        pipe -- The Redis pipeline to which we write the initialization.

        """
        # Groom some inputs.
        if not by_scores:
            if '+inf' == start:
                start = 0
            if '-inf' == end:
                end = -1
        # Get the data requested from SQL.
        query = self.get_sql_query()

        # If we inited the start of the set we mark the set inited.
        if (by_scores and start == '+inf') or (not by_scores and start == 0):
            self.add_init(pipe=pipe)

        score_attr = self.get_score_persistent_attribute()
        # Add filters for start and end.
        if by_scores:
            if start != '+inf':
                query = query.filter(score_attr <= from_hsntime(start))
            if end != '-inf':
                query = query.filter(score_attr > from_hsntime(end))
        # If not by_scores we need to get all the items to know their rank.

        # The query must be in order.
        query = query.order_by(desc(score_attr))

        if by_scores:
            if count is not None:
                persistent_items = query[:count + 1]
            else:
                persistent_items = query.all()
        else:
            if -1 == end:
                persistent_items = query[start:]
            else:
                persistent_items = query[start:end + 1]

        # Promote the SQL data to backend Entities.
        entries = [self.persistent_to_entry(p) for p in persistent_items]

        # We asked for count + 1, did we get that many?  If so, some items
        # remain beyond the user's query.
        if count is None:
            more = False
        else:
            more = (count + 1) == len(entries)
        if more:
            add_these = entries[:-1]
        else:
            add_these = entries

        # Add the items to Redis.
        for entry, score in add_these:
            self.add(entry, score, pipe=pipe)

        # Add the remaining marker.
        if more:
            score = entries[-1][1]
            self.add_remaining_marker(score, pipe=pipe)
        else:
            self.add_none_remain_marker(pipe=pipe)

        return add_these

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
