
import logging

from backend.datastructures import IncompleteSet, MemorySet
from backend.exceptions import AttributeNotLoaded, DuplicateKeyError


logger = logging.getLogger("hsn.backend")

# Note that delta.apply is only used in one place in backend.environments.cache
# it used to be used by the ORM save in backend.environments.persistent,
# but now that it is no longer used for that the ability for delta.apply to
# be used in such case is lost.  (and along with that delta no longer
# imports EnvironmentPersist, preventing certain circular import headaches.)

class Delta(object):
    """Object representing an attribute change in an Entity/Environment."""

    def __init__(self, entity, attribute, value):
        #import traceback
        #traceback.print_stack()
        if isinstance(value, (IncompleteSet, MemorySet)):
            value = value.copy()
        self.attribute = attribute
        self.value = value
        self.entity = entity

    def apply(self, environment):
        """Apply this delta to the environment."""
        raise NotImplementedError

    @staticmethod
    def _serialize_sql_entity(entity):
        try:
            sql_key = getattr(entity, entity.entity_class.sql_primary_key_attribute.name)
        except AttributeNotLoaded:
            sql_key = None
        entity_data = (entity.entity_class.__name__,
                       entity.key,
                       entity.write_only,
                       entity.created,
                       sql_key,)
        return entity_data

    @staticmethod
    def _serialize_cassandra_entity(entity):
        # Primary keys are UUIDs or strings, which encode unchanged.
        cassandra_key = entity.key
        entity_data = (entity.entity_class.__name__, cassandra_key)
        return entity_data

    def serialize_sql(self):
        from backend.environments.core import Entity
        entity_data = Delta._serialize_sql_entity(self.entity)# [self.entity.entity_class.__name__, self.entity.key, self.entity.write_only]
        if not self.attribute:
            return (self.type_id, entity_data,)
        attribute_data = self.attribute.name
        if self.attribute.enum and self.attribute.is_set_like:
            value_data = set([x.number for x in self.value])
        elif self.attribute.enum:
            if self.value is None:
                value_data = None
            else:
                value_data = self.value.number
        elif self.attribute.related_attribute:
            # Could be an append so may not be a set.
            if self.value is None:
                value_data = None
            elif isinstance(self.value, Entity):
                if self.value.persistent_class.saves_to_postgresql:
                    value_data = Delta._serialize_sql_entity(self.value)
                else:
                    value_data = self.value.key
            else:
                value_data = set([Delta._serialize_sql_entity(e) for e in self.value])
        else:
            value_data = self.value
        return (self.type_id, entity_data, attribute_data, value_data,)

    def serialize_cassandra(self):
        from backend.environments.core import Entity
        entity_data = Delta._serialize_cassandra_entity(self.entity)# [self.entity.entity_class.__name__, self.entity.key, self.entity.write_only]
        if not self.attribute:
            return (self.type_id, entity_data,)
        attribute_data = self.attribute.name
        # xxx this could use cassandra_encode for enumsetfield, yes?
        # xxx what if this is an append to the enumset, do we allow that?
        if self.attribute.enum and self.attribute.is_set_like:
            value_data = set([x.number for x in self.value])
#        elif self.attribute.related_attribute:
#
#            # Could be an append so may not be a set.
#            if self.value is None:
#                value_data = self.attribute.cassandra_encode(None)
#            elif isinstance(self.value, Entity):
#                value_data = encode(self.value.key)
#            else:
#                value_data = set([encode(e.key) for e in self.value])
        else:
            value_data = self.attribute.cassandra_encode(self.value)
        return (self.type_id, entity_data, attribute_data, value_data,)


class SetAttribute(Delta):
    """A Delta indicating an attribute is having its value set."""
    value_is_relative = False
    type_id = 0

    def apply(self, environment):
        environment.set_attribute(self.entity, self.attribute, self.value)

    def __repr__(self):
        return "SetAttribute(%s.%s = %s)" % (self.entity,
                                             self.attribute.name,
                                             self.value)

class IncrementAttribute(Delta):
    """A Delta indicating an attribute is to have its value incremented."""
    value_is_relative = True
    type_id = 10

    def apply(self, environment):
        v = environment.get_attribute(self.entity, self.attribute)
        environment.set_attribute(self.entity, self.attribute, self.value + v)

    def __repr__(self):
        return "IncrementAttribute(%s.%s = %s)" % (self.entity,
                                                   self.attribute.name,
                                                   self.value)


class CreateEntity(Delta):
    value_is_relative = False
    type_id = 1

    def __init__(self, entity):
        from backend.environments.core import Entity
        assert isinstance(entity, Entity)
        self.attribute = None
        self.value = None
        self.entity = entity
        self.counterpart_delta = None # Relationships have this.

    def apply(self, environment):
        # If this item already exists in this environment, raise an exception.
        get = environment.get_peer(self.entity)
        if get is not None:
            raise DuplicateKeyError("Attempting to create an object (%s) with a key (%s) that already exists in environment %s" % (self.entity.__class__, self.entity.key, environment))
        environment.fetch_peer(self.entity)

    def __repr__(self):
        return "CreateEntity(%s)" % (self.entity)


class DeleteEntity(Delta):
    value_is_relative = False
    type_id = 2

    def __init__(self, entity):
        from backend.environments.core import Entity
        assert isinstance(entity, Entity)
        self.attribute = None
        self.value = None
        self.entity = entity
        self.counterpart_delta = None # Relationships have this.

    def apply(self, environment):

        # If this item already exists in this environment, raise an exception.
        get = environment.get_peer(self.entity)
        if get is None:
            logger.debug("__ses__ not deleting because it does not exist in this environment.")
            return
        environment.delete(get)

    def __repr__(self):
        return "DeleteEntity(%s)" % (self.entity)


class AppendOneToMany(Delta):
    value_is_relative = True
    type_id = 4

    def __init__(self, this_obj, this_attribute, that_obj, that_attribute):
#        self.obj = this_obj
#        self.attribute = this_attribute
#        self.value = that_obj
        self.value_attribute = that_attribute
        super(AppendOneToMany, self).__init__(this_obj, this_attribute, that_obj)

    def apply(self, environment):
        entity = environment.fetch_peer(self.entity)
        value = self.value
        if self.attribute.enum:
            pass
        else:
            value = environment.fetch_peer(self.value)
        # Perform this delta on the given entity.
        if entity and value:
            attr_name = self.attribute.name
            # I don't think the try is necessary here as you'll get a
            # write-only sethandle if it does not exist.
            try:
                getattr(entity, attr_name).add(value)
            except AttributeNotLoaded:
                pass

    def __repr__(self):
        return "AppendOneToMany(%s.%s = %s)" % (self.entity,
                                                self.attribute.name,
                                                self.value)


class DiscardOneToMany(Delta):
    value_is_relative = True
    type_id = 5

    def __init__(self, this_obj, this_attribute, that_obj, that_attribute):
#        self.obj = this_obj
#        self.attribute = this_attribute
#        self.value = that_obj
        self.value_attribute = that_attribute
        super(DiscardOneToMany, self).__init__(this_obj, this_attribute, that_obj)

    def apply(self, environment):
        entity = environment.fetch_peer(self.entity)
        value = self.value
        if self.attribute.enum:
            pass
        else:
            value = environment.fetch_peer(self.value)
        # Perform this delta on the given entity.
        if entity and value:
            # Special case:
            attr_name = self.attribute.name
            try:
                listlike = getattr(entity, attr_name)
                listlike.discard(value)
            except AttributeNotLoaded:
                pass


class AppendManyToMany(Delta):
    value_is_relative = True
    type_id = 8

    def __init__(self, this_obj, this_attribute, that_obj, that_attribute):
#        self.obj = this_obj
#        self.attribute = this_attribute
#        self.value = that_obj
        self.value_attribute = that_attribute
        super(AppendManyToMany, self).__init__(this_obj, this_attribute, that_obj)

    def apply(self, environment):
        entity = environment.fetch_peer(self.entity)
        value = self.value
        if self.attribute.enum:
            pass
        else:
            value = environment.fetch_peer(self.value)
        # Perform this delta on the given entity.
        if entity and value:
            # Special case:
            attr_name = self.attribute.name
            try:
                try:
                    getattr(entity, attr_name).add(value)
                except AttributeError:
                    getattr(entity, attr_name).append(value)
#                logger.debug("__ses__ zoeuoeuoeunth")
            except AttributeNotLoaded:
#                logger.debug("__ses__ attribute not loaded from apply???")
                pass

    def __repr__(self):
        return "AppendManyToMany(%s.%s append %s)" % (self.entity,
                                                      self.attribute.name,
                                                      self.value)

class DiscardManyToMany(Delta):
    value_is_relative = True
    type_id = 9

    def __init__(self, this_obj, this_attribute, that_obj, that_attribute):
#        self.obj = this_obj
#        self.attribute = this_attribute
#        self.value = that_obj
        self.value_attribute = that_attribute
        super(DiscardManyToMany, self).__init__(this_obj, this_attribute, that_obj)

    def apply(self, environment):
        entity = environment.fetch_peer(self.entity)
        value = self.value
        if self.attribute.enum:
            pass
        else:
            value = environment.fetch_peer(self.value)
        # Perform this delta on the given entity.
#        logger.debug("__ses__ entity and value: %s %s" % (entity, value))
        if entity and value:
            # Special case:
            attr_name = self.attribute.name
            try:
#                logger.debug("__ses__ applying like so: getattr(%s, %s).remove(%s)" % (entity, attr_name, value))
                listlike = getattr(entity, attr_name)
                listlike.discard(value)
            except AttributeNotLoaded:
                pass


TYPE_ID_TO_DELTA_TYPE = {
    SetAttribute.type_id: SetAttribute,
    CreateEntity.type_id: CreateEntity,
    DeleteEntity.type_id: DeleteEntity,
    AppendOneToMany.type_id: AppendOneToMany,
    DiscardOneToMany.type_id: DiscardOneToMany,
    AppendManyToMany.type_id: AppendManyToMany,
    DiscardManyToMany.type_id: DiscardManyToMany,
    IncrementAttribute.type_id: IncrementAttribute
}
