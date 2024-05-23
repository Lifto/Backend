import logging
logger = logging.getLogger("hsn.backend")
logger.debug("init backend")

from backend.core import (init_backend, new_active_session,
    get_active_session, close_active_session, save, load, generate_id)
from backend.environments.persistent import MainPersistent
from backend.environments.cassandra import Cassandra
from backend.async import queue_async, queue_async_later, with_save