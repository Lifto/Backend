
"""Make Caches for storing memory-managed zsets, sets and counts in Redis.

These Caches map an 'owner' (whose key is the key to a particular cache 
instance) to a set of cache 'entries'.  These Cache classes provide an
implementation that can be used by simply declaring some class_variables like
the owner_class, entry_class, score_attribute_name etc.  One may also override
certain functions to get the cache functionality they need (see the 'override'
section in each Cache module.)

An example of a cache that uses the default functionality

class PostCommentsCache(ZSetCache):
    "Partial cache of a Post's Comments, sorted by time posted."
    key_base = '\x0c'
    owner_class = Post
    entry_class = PostComment
    score_attribute_name = 'time_posted'
    load_patch = '_comments'
    entry_join_attribute_name = 'post_uuid'
    owner_join_attribute_name = 'uuid'

This cache looks up objects in SQL by joining Post.uuid with
PostComment.post_uuid and sorting by PostComment.time_posted.

'load_patch' indicates that the results should be placed in a dynamic
attribute on the 'owner' (which is a Post entity) under the name '_comments'.

The results will be a list of PostComment entities ('entries').

Note that nothing is promoted by default, it is just an instance of the entry.

Memory management is invoked in logic.evict

To implement more sophisticated caches look at the Cache module of the Cache
that you want to use.  There is a section marked 'override these', which lets
you override the SQL query, the initter, or any of the serializers.  If you
override these then certain of the configuring class variables need not be
defined.

"""
