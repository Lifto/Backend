
"""Middleware for backend's writes and the giving to Celery of queued tasks."""

import logging

import backend

logger = logging.getLogger("hsn.backend")

        
class BackendMiddleware(object):

    def process_response(self, request, response):
        # Get default environment from backend, if there is one.
        session = backend.core.get_active_session_or_none()
        if session:
            # Apply or queue all stored deltas, auto-triggred saves,
            # and queued refreshes.
            session.save()

        # Close the environment, it gets created on next use.
        backend.core.close_active_session()
        return response

    def process_exception(self, request, exception):
        msg = 'Backend middleware is processing an exception, resetting.\n%s\n%s\n%r' % (request.path, request.build_absolute_uri(), request)
        if getattr(request, 'skip_exc_print', False):
            logger.error(msg)
        else:
            logger.exception(msg)
            request.skip_exc_print = True
            
        backend.core.close_active_session()
        return None
