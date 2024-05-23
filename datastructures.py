
from backend.exceptions import IncompleteSetNotLoaded

import logging
logger = logging.getLogger("hsn.backend")


class MemorySet(set):

    def __init__(self, *args, **kwargs):
        self._modifiers = set()
        super(MemorySet, self).__init__(*args, **kwargs)

    def get_one(self):
        return self.__iter__().next()


class IncompleteSet(object):
    """
    A set that knows what has been added and removed from it but may not
    know its size or what its contents may otherwise be.

    Note that the size returned may be incorrect if the actual already holds
    a particular value not held by this incomplete set and that same value is
    added locally. (And similarly with discards.)

    """

    def __init__(self, contains=None, does_not_contain=None, size=None):
        if contains is None:
            self._contains = set()
        else:
            assert isinstance(contains, set)
            self._contains = contains
        if does_not_contain is None:
            self._does_not_contain = set()
        else:
            assert isinstance(does_not_contain, set)
            self._does_not_contain = does_not_contain
        if size is not None:
            self._size = size
        else:
            self._size = None # None means size is unknown.
        self._modifiers = set()

    def set_size(self, size):
        """
        Initialize size-tracking in this IncompleteSet.

        This value is returned when len is requseted. Adding or discarding
        elements alters this size. (Note that if the incomplete set is not
        aware that the set already contains (or has already removed) value X,
        then adding or removing value X may erroneously change the size
        returned by len.)
        
        """
        self._size = size

    def add(self, value):
        # We need to be sure to not bump the size if the item is already
        # in the set.
        if not self._size is None and value not in self._contains:
            self._size += 1
        self._does_not_contain.discard(value)
        self._contains.add(value)
        # Note that an incomplete set may record a size increment incorrectly
        # if the value added is known to be present in the set in the
        # authoritatize source but not known to be present in this
        # IncompleteSet.

    def discard(self, value):
        # We need to be sure to not debump the size if the item is already not
        # in the set.
        if not self._size is None and value not in self._does_not_contain:
            logger.debug("Decrementing SIZE----------------------------")
            self._size -= 1
        self._contains.discard(value)
        self._does_not_contain.add(value)

    def append(self, *args, **kwargs):
        raise NotImplementedError(
            "append not implemented for IncompleteSet. Use 'add'")

    def remove(self, *args, **kwargs):
        raise NotImplementedError(
            "remove not implemented for IncompleteSet. Use 'discard'")

    def clear(self):
        raise NotImplementedError('clear not implemented for IncompleteSet')

    def difference(self, *args, **kwargs):
        raise NotImplementedError(
            'difference not implemented for IncompleteSet')

    def difference_update(self, x):
        raise NotImplementedError(
            'difference_update not implemented for IncompleteSet')

    def __contains__(self, value):
        if value in self._contains:
            return True
        if value in self._does_not_contain:
            return False
        raise IncompleteSetNotLoaded

    def get_one(self):
        if self._contains:
            # TODO: can do this with __iter__ on self._contains?
            logger.warn("IncompleteSet.get_one can do this with __iter__ on self._contains?")
            return list(self._contains)[0]
        else:
            # Not the right error, set could just be empty.
            raise IncompleteSetNotLoaded

    def __nonzero__(self):
        if self._contains:
            return True
        else:
            raise IncompleteSetNotLoaded

    def __eq__(self, other):
        raise NotImplementedError('__eq__ not implemented for IncompleteSet')

    def __len__(self):
        logger.debug("incomplete set len %s" % self._size)
        if self._size is None:
            raise IncompleteSetNotLoaded
        return self._size

    def __cmp__(self, *args, **kwargs):
        raise TypeError

    def __and__(self, *args, **kwargs):
        raise NotImplementedError('__and__ not implemented for IncompleteSet')

    def __or__(self, *args, **kwargs):
        raise NotImplementedError('__or__ not implemented for IncompleteSet')

    def __sub__(self, *args, **kwargs):
        raise NotImplementedError('__sub__ not implemented for IncompleteSet')

    def __iter__(self):
        raise NotImplementedError('__iter__ not implemented for IncompleteSet')

    def __repr__(self):
        return "IncompleteSet(%s,%s,%s)" % (self._contains,
                                            self._does_not_contain,
                                            self._size)

def patch(target, source):
    if isinstance(target, IncompleteSet):
        if isinstance(source, MemorySet):
            new_target = MemorySet(list(source))
            source = target
            target = new_target
    if isinstance(source, IncompleteSet):
        for v in source._contains:
            target.add(v)
        for v in source._does_not_contain:
            target.discard(v)
    elif isinstance(source, MemorySet):
        for v in source:
            target.add(v)
    return target
