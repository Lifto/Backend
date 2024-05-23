# Backend
A Python multi-source ORM with a multithreaded asynchronous Cassandra loader.

Backend is a custom ORM for a social network, written in Python, circa 2013. It manages a store of persistent data in a SQL database, and in Cassandra, and caches that data in Redis. Backend objects inherit from `Entity` (see `schema.py`). Backend stores local changes in a session (`session.py`). The middleware saves new data to the Redis cache before exiting, and enques to celery a command that will save the new data to SQL after the middleware returns. A special feature of Backend is a loader that holds many simultaneous load requests to Cassandra in coroutines, and eliminates all redundant load requests (ex: if there is a request to load two objects both of which hold a reference to the same User object, that User object will only be loaded once.)


