
"""A system for queueing asynchronous tasks.

Terminology:
Celery -- An asynchronous processing system.
Task -- A unit of work to be executed at a later time.
Encelerate -- The giving of a task to Celery for execution.  (Note that
    Celery has its own sophisticated means of ordering, retrying, and 
    timing-out, etc.)  This term is used for clarity in this documentation.
Queue -- Adding a task to a queue so that it may be encelerated at
    a particular time. (queues available are 'now' and 'later')

'now' and 'later' tasks:
Tasks can be put into two queues, one whose tasks are encelerated during the
conclusion of middleware (called 'now'), and one that encelerates following the
completion of Backend's asynchronous save (called 'later'.)  The function
queue_async puts a task on the 'now' queue and the function queue_async_later
puts a task on the 'later' queue.

Tasks in either the 'now' or 'later' queues are both eventually encelerated.
The distinction is in _when_ they encelerate. A 'now' task encelerates on
middleware exit, after Backend handles its own 'now' saves.  'later' tasks are
not encelerated until Backend's own encelerated 'save_later' concludes.  The
reason there is a 'later' queue is so that a task that depends on Backend's
data to have finished (asynchronously) writing to storage will not encelerate
until that writing is complete.

A 'later' task takes the following path:
1: queue to 'later'.
2: On middleware exit, the 'later' queue is packaged into Backend's
    'save_later' task.
3: Backend's 'save_later' task is encelerated.
4: Backend's 'save_later' task executes asynchronously on Celery.
5: Tasks in the packaged-in 'later' queue are then themselves encelerated.

A 'now' task takes a much simpler path:
1: Queue to 'now'.
2: Encelerate on middleware exit.

Special case -- When there is no Backend 'save_later'.
In this case both the 'now' and 'later' tasks are encelerated during
middleware exit.

Note that there are no circumstances under which a task is executed
synchronously. The 'now' and 'later' distinctions refer to when the tasks are
encelerated, not when they are executed. (Celery has a sophisticated system of
managing its own queues of tasks. 'encelerate' just means that Celery is now in
control of the task, not that the task is or has executed.)

"""
# TODO: Make an addendum to that paragraph to account for use_celery=False.

# Async is a feature of backend. It is not that we have the ability to put
# things on celery, that is trivial. The backend provides the ability to
# finely control when tasks are made active in celery vis-a-vis writes.

# Note - The session calls the queue processing because proper usage is
# so tightly bound to how the session saves.

# TODO: Make this a generic system and have the session hold on to its own
# async queues, which get cleared and reset as needed. Remove async 
# middleware, and include the clearing of the queues as part of the backend
# middleware.

# Should third class storage be here or in session? (currently we only do
# third class storage with a redis pipeline, but it could be for any storage.)

# Should third class storage go out with async save if there is an async save
# and not a sync save? TODO.

# fourth class storage should be something like the logs, that writes to a
# local system and not to redis. Perhaps even logs would use it, and instead
# we could have 'timing statements' that are real time, but otherwise logs
# can safely be written in a buffered manner.

import sys
import threading
import logging
from datetime import timedelta

from decorator import decorator
from psycopg2 import OperationalError
from celery.registry import tasks as task_registry
from django.conf import settings

from backend import new_active_session, get_active_session
from backend.core import close_active_session
from util.when import now, stringify
from util.admin import send_admin_error_email_no_req

logger = logging.getLogger("hsn.backend")

NOW_QUEUE = threading.local()
LATER_QUEUE = threading.local()

def _pop_now_queue():
    """Pop the 'now' asynchronous task queue."""
    return _pop_async_tasks(NOW_QUEUE)

def _pop_later_queue():
    """Pop the 'later' asynchronous task queue."""
    return _pop_async_tasks(LATER_QUEUE)

def queue_async(task, args=None, kwargs=None, routing_key=None, countdown=None, **options):
    """Queue 'task' for asynchronous processing.

    The task will be given over to Celery upon exit of the middleware.

    Tasks enqueued with this method are given over to Celery after Backend
    completes the 'now' portion of its save in middleware.

    If options contains 'use_celery=False' the task will be run locally.

    """
    _queue_task(NOW_QUEUE, task, False, args, kwargs, routing_key, countdown=countdown, **options)

def queue_async_later(task, args=None, kwargs=None, routing_key=None, countdown=None, **options):
    """Queue 'task' for asynchronous processing.

    Tasks enqueued with this method are given over to Celery after Backend
    completes the 'later' (asynchronous) portion of its save operation.

    Note that this task will be encoded as an argument to the Celery call
    that asynchronously processes the Backend's save. When that save is
    complete the encoded task list is then given over to Celery. This means
    that this task is given over to celery from a celery thread, not the
    webrequest from which it was first enqueued. In the case where
    process_async is called when the Backend has not queued its async save
    the tasks queued with queue_async_later are given over to Celery when
    the 'queue_async' tasks are given over to Celery.

    """
    _queue_task(LATER_QUEUE, task, False, args, kwargs, routing_key, countdown=countdown, **options)

# Used internally to make our queuable task object.
def _queue_task(threadlocal_queue, task, is_backend=False, args=None,
                kwargs=None, routing_key=None, countdown=None, **options):
    """Add 'task' to the 'threadlocal_queue'."""
    logger.debug('task: %s' % task)
    try:
        task_data = {'task': task.name}
    except AttributeError:
        # This is only allowable if the task is to be run locally.
        assert 'use_celery' in options and not options['use_celery']
        assert threadlocal_queue == NOW_QUEUE
        task_data = {'task_function': task}
    task_data.update({'is_backend':is_backend, 'args':args, 'kwargs':kwargs,
                      'routing_key':routing_key, 'countdown':countdown, 'options':options})
    if not hasattr(threadlocal_queue, 'tasks'):
        threadlocal_queue.tasks = []
    threadlocal_queue.tasks.append(task_data)

def _pop_async_tasks(threadlocal_queue):
    """Return the tasks from the given queue and clear the queue."""
    if hasattr(threadlocal_queue, 'tasks'):
        tasks = threadlocal_queue.tasks
        threadlocal_queue.tasks = []
        return tasks
    else:
        return []

# Called by the middleware when an exception is raised.
def clear_async_tasks(clear_now=True, clear_later=True):
    """Clear the given asynchronous task queues."""
    if clear_now:
        _pop_now_queue()
    if clear_later:
        _pop_later_queue()

def process_now_queue():
    _process_async_tasks(_pop_now_queue())

def process_later_queue():
    _process_async_tasks(_pop_later_queue())

def _process_async_tasks(task_queue):
    """Give the tasks over to Celery.

    task_queue is expected to hold data added by _queue_task.

    """
    backend_handled = False
    if task_queue:
        for task_data in task_queue:
            if 'task' in task_data:
                task = task_registry[task_data['task']]
                task_args = task_data.get('args', ())
                task_kwargs = task_data.get('kwargs', {})
                apply_kwargs = task_data.get('options', {})
                # Just put routing_key in options if available - we made explicit for visibility
                if 'routing_key' in task_data:
                    apply_kwargs['routing_key'] = task_data['routing_key']
                if 'countdown' in task_data:
                    apply_kwargs['countdown'] = task_data['countdown']
                try:
#                    if settings.ASYNC_ENABLED:
                    task.apply_async(task_args,
                                     task_kwargs,
                                     **apply_kwargs)
#                    else:
#                        logger.debug("kwargs %s" % task_data['kwargs'])
#                        logger.debug("options %s" % task_data['options'])
#                        task_kwargs = {}
#                        task_kwargs.update(task_data['kwargs'] or {})
#                        task_kwargs.update(task_data['options'] or {})
#                        logger.debug("new wargs %s" % task_kwargs)
#                        task(*task_data['args'] or [], **task_kwargs)
                except:
                    logger.exception('Problem dispatching async. Check that rabbitmq service is running and has users/vhosts configured.')
                    import util.admin
                    util.admin.send_admin_error_email_no_req("Async dispatch problem (Rabbitmy down or unconfigured?)")
                    # XXX if it's just a timestamp update (which is often the only thing necessary for reading feeds),
                    #    we could log the error and move on.  This would keep the site closer-to-working
                    raise
            else:
                # This is to handle a non-async function.
                task = task_data['task_function']
                args = task_data['args'] or []
                kwargs = task_data['kwargs'] or {}
                task(*args, **kwargs)

    return backend_handled

# Decorator.
def with_save(retry=False):
    """Decorator supplying a middleware equivalent for asynchronous tasks.

    Functions decorated with_save(retry=True) will be retried if they raise
    exceptions.  The decorated method will be called again with retry_data and
    the exception that caused the retry.  Decorated functions can examine their
    keyword arguments for retry information.

    This decorator calls the session's save at the conclusion of the task.
    Any retries caused by errors in this save will have their retries
    automatically handled by this decorator.  In such case the function is not
    called again with retry data because the retry is backend-internal. (note
    this only applies to the 'later' saves, the 'now' saves still raise
    exceptions that cause the entire task to retry.)

    Note that the 'later' saves will be attempted on this thread serially
    but if an exception is raised they will be retried on the backend update
    exchange.

    """
    from backend.environments.persistent import MainPersistent
    def decorator_maker(target):
        def wrapper(target, *args, **kwargs):
            # We prepare a backend environment inside of a try block. If an
            # exception occurs we free the backend environment's resources.
            try:
                logger.debug('@with_save begin %s' % target.func_name)
                start_time = now()

                # Begin a new session for this function to use.
                new_active_session()

                # Call the target function.
                retval = target(*args, **kwargs)

                # Run middleware-esque functions that apply to celery tasks.
                from logic.notify.process import detect
                detect()

                # Run the session save as the middleware would in a web request.
                session = get_active_session()
                session.save(try_later_on_this_thread_first=True)
                logger.debug('@with_save %s complete in %s' % (
                    target.func_name, stringify(now()-start_time)))

                return retval
            except Exception, e:
                if 'task_name' not in kwargs:
                    logger.exception("Not in task context, skipping retry logic on exception")
                    raise

                # In the case of an exception, we either fail or retry.
                name = kwargs['task_name']
                retries = kwargs['task_retries']

                # In order to retry the argument to @with_save must indicate
                # that retry is allowed.  If this exception has a 'retry'
                # attribute set to False it can not be retried.
                if retry and not (hasattr(e, 'retry') and not e.retry):
                    # Get the amount of time celery will hold this task
                    # before retrying.
                    countdown = _get_timeout(retries)
                    if retries < settings.MAX_RETRIES:                        
                        # If we haven't retried too many times, and if it is
                        # going to be the second retry, send the admins an
                        # email.
                        logger.debug("@with_save queing RETRY task(%s, %s) %s in %s seconds" % (
                            args, kwargs, name, countdown))
                        if retries == 2:
                            send_admin_error_email_no_req(
                                "%s %s %s raised %s, retrying" % (
                                    name, args, kwargs, type(e)),
                                sys.exc_info())
                    else:
                        # If we retried too many times, send the admins an
                        # email that we are about to queue a retry that
                        # celery will reject.
                        logger.debug("@with_save queing RETRY task(%s, %s) %s, will fail" % (
                            args, kwargs, name))
                        send_admin_error_email_no_req(
                            "%s %s %s raised %s, retry will fail" % (
                                name, args, kwargs, type(e)),
                            sys.exc_info())

                    # Have celery retry this failed task. If the retry count
                    # is exceded, celery will raise an exception.
                    task_function = _get_function_for_name(name)
                    task_function.retry(args=args, kwargs=kwargs,
                        exc=e, # Exception to raise if max-retry.
                        countdown=countdown)
                else:
                    # This is not a retryable task, re-raise the exception.
                    logger.error("@with_save fail task %s(%s, %s)" % (
                        name, args, kwargs))
                    send_admin_error_email_no_req("Fail Task %s(%s, %s)" % (
                        name, args, kwargs), sys.exc_info())
                    logger.exception("re-raising %s" % e)
                    raise
            finally:
                # Always finish the SQL session and free top-level references
                # so that garbage may be collected.
                logger.debug('@with_save finally')
                session = None
                close_active_session()
                # Note, use backend.core.remove_session or else testing borks.
                import backend.core
                backend.core.remove_session()
                clear_async_tasks()
        return decorator(wrapper)(target)
    return decorator_maker

#A decorator for a celery task that requires a new active session when
# loaded, but doesn't save anything
def read_only_session():
    """Decorator to provide a new active session.

    Doesn't call save on the backend.

    """
    def db_decorator(target):
        def wrapper(target, *args, **kwargs):
            try:
                name = kwargs['task_name'] if 'task_name' in kwargs else str(target)
                retry_count = kwargs['task_retries'] if 'task_retries' in kwargs else 0
                logger.debug("read_only_session %s (%d)" % (name, retry_count))

                #Start new session
                new_active_session()

                #Run target
                retval = target(*args, **kwargs)

                #Done
                return retval
            except:
                import backend.core
                backend.core.close_session()
                raise
            finally:
                # Note, use backend.core.remove_session b/c testing overrides.
                import backend.core
                backend.core.close_active_session()
                backend.core.remove_session()
                clear_async_tasks()
        return decorator(wrapper)(target)
    return db_decorator

def _get_timeout(retry):
    """Given the current retry, return seconds to wait for next retry.

    How long to wait before retrying the first attempt?
    >>> _get_timeout_for_retry(0)
    10

    If the second attempt (retry 1) fails, how long before another retry?
    >>>_get_timeout_for_retry(1)
    60

    """
    try:
        backoff = settings.BACKOFF_LIST[retry]
    except IndexError:
        backoff = settings.BACKOFF_LIST[-1]
    if settings.EXPONENTIAL_BACKOFF:
        return backoff
    else:
        t = stringify(timedelta(seconds=backoff))
        logger.debug("backoff disabled, would have been %s" % t)
        try:
            return settings.DISABLED_BACKOFF_LIST[retry]
        except IndexError:
            return settings.DISABLED_BACKOFF_LIST[-1]

def _get_function_for_name(name):
    parts = name.split('.')
    module = ".".join(parts[:-1])
    m = __import__(module)
    for comp in parts[1:]:
        m = getattr(m, comp)
    return m
