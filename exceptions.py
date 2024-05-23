
import logging
logger = logging.getLogger("hsn.backend")

# TODO: Inherit from BaseException
# see http://www.python.org/dev/peps/pep-0352/
class BackendError(Exception):
    """Baseclass for all Backend Errors."""

class DuplicateKeyError(BackendError):
    retry = False

class EmptyKeyError(BackendError):
    """Raised when save is made with an empty value for a (composite) key."""
    retry = False

class AssignmentNotAllowed(BackendError):
    """Raised on attempt to assign value to a non-None composite key."""

class AllAttributesNotLoaded(BackendError):
    """
    Raised when assigning a value to a composite key attribute would result in
    a save where not all of the non-increment attributes for the entity are
    known (loaded or promoted).

    """

class KeyNotLoaded(BackendError):
    """
    Raised when attempting to assign a value to a composite key attribute and
    the value of that attribute is not known.

    Remedy is to load (or patch) the existing value for that attribute before
    assignment.

    """

class EntityNotFoundError(BackendError):
    pass

class NoDataLoaded(BackendError):
    pass

class AttributeNotLoaded(BackendError):
    pass

class IncompleteSetNotLoaded(AttributeNotLoaded):
    pass

class NullEnvironmentError(BackendError):
    pass

class IllegalCommand(BackendError):
    pass

class IllegalSchema(BackendError):
    """Raised when a not allowed declaration is made in the model."""

class UninitializedLoader(BackendError):
    """Raised when one instantiates a Loader that is not initialized."""
    # This can happen if your Loader is not in model.caches.LOADERS or if
    # init_backend() was never called (it's called automatically in the web
    # server and in hsn shell so this latter case is only likely if you are
    # importing the backend from a script.)

class WriteOnlyAccessForbidden(BackendError):
    """A feature forbidden to write-only objects was accessed."""

class InventoryError(BackendError):
    """A Cache's memory is out of compliance with its inventory."""

class ExcessInventoryError(InventoryError):
    """A Cache has data that is not supposed to be present."""

class RedisLoadError(BackendError):
    pass

class NotAccelerable(BackendError):
    pass

class TooManyDeltas(BackendError):
    """Raised when a new delta brings the count over BACKEND_MAX_DELTAS."""
