
from itertools import chain
import logging
import threading

from django.conf import settings
import elixir
from elixir import EntityMeta
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.collections import InstrumentedList

from util.cache import get_existence_redis
from util.helpers import pluralize
from util.when import now, stringify
from backend import generate_id
from backend.core import retry_operational_error
from backend.datastructures import MemorySet
from backend.environments.cassandra import Cassandra
from backend.environments.core import Entity, Environment
from backend.environments.memory import SetHandle
from backend.exceptions import (EntityNotFoundError, AttributeNotLoaded,
    NotAccelerable, DuplicateKeyError)
from backend.schema import Persistent


logger = logging.getLogger("hsn.backend")


class EnvironmentPersistent(Environment):
    """The environment is the primary way to interact with SQL Alchemy. It
    keeps a set of accessed Persistent objects to prevent needing to query for
    them as they are used.

    """

    def __init__(self, entity=None):
        # This is a dict of the temporary keys and the Persistent objects
        # created to fulfill them.
        # [entity_class name][key] = Persistent instance
#        self.temporary_instance_dict = {} # type to tempkey-entity dict dict
        self.persistents = {} # [entity_class.__name__][key] = Persistent instance.
        super(EnvironmentPersistent, self).__init__()

    def _add_to_persists(self, persistent):
        key = getattr(persistent,
                      persistent.entity_class.key_attribute_name)
        by_key = None
        try:
            by_key = self.persistents[persistent.entity_class.__name__]
        except KeyError:
            by_key = {}
            self.persistents[persistent.entity_class.__name__] = by_key
        assert key not in by_key
        by_key[key] = persistent

    def _get_from_persists(self, entity_class, key):
        by_key = None
        try:
            by_key = self.persistents[entity_class.__name__]
        except KeyError:
            return None
        instance = None
        try:
            instance = by_key[key]
        except KeyError:
            return None
        return instance

    def _reset_persists(self):
        self.persistents = {}

    def create(self, entity_class, key):
        created = None
        if key:
            created = entity_class.persistent_class()
            setattr(created, entity_class.key_attribute_name, key)
        else:
            logger.warn("creating %s without key" % entity_class)
            created = entity_class.persistent_class()
        assert created is not None
        self._add_to_persists(created)
        return created

    @retry_operational_error()
    def get(self, entity_class, key):
        # Get does not check temporaries because it can only be called with
        # a real key.
        # If we have this one, return it.
        instance = self._get_from_persists(entity_class, key)
        if instance is None:
            instance = entity_class.persistent_class.get_by(**{entity_class.key_attribute_name:key})
            if instance is not None:
                self._add_to_persists(instance)
            return instance
        else:
            return instance

    # TODO: We could attempt to keep a local ref? It may be faster even though
    # SQA supposedly supports identity.
    def get_peer(self, entity):
        assert(entity is not None)
        if isinstance(entity.environment, EnvironmentPersistent):
            assert entity.environment is self # At present there is only one.
            # Any Persistent is the same.
            return entity
        return self.get(entity.entity_class, entity.key)

    @classmethod
    def from_peer(cls, entity=None):
        return cls()

    @classmethod
    def add_to_session(self, session):
        session.loaded_environments.append(self())

    def get_attribute(self, entity, attribute):
        entity = self.get_peer(entity)
        if entity is None:
            logger.debug("Entity not found in Sql: %s" % entity)
            # This can happen as a result of a race condition due to the gap
            # in update time between the optimized feed storage and the
            # backend. It means we are trying to set a value on an entity we
            # haven't created yet. It may have been created, and entered into
            # feed storage, and then modified by another user who read it
            # from feed storage, and this is that modification arriving
            # before the create.
            raise EntityNotFoundError("%s" % entity)
        value = getattr(entity, attribute.name)
        if value is None:
            # XXX this is a hack REMOVE after migrating away ENUM NONES xxx MEC
            # xxx and incrs - mec
            if attribute.enum and not attribute.is_set_like:
                return attribute.enum[0]
            elif type(attribute).__name__ == 'Incrementer':
                return 0            
            return None
        if str == attribute.value_class:
            value = str(value)
        elif bool == attribute.value_class:
            value = bool(value)
        elif int == attribute.value_class:
            value = int(value)
        elif float == attribute.value_class:
            value = float(value)
        return value

    def set_attribute(self, entity, attribute, value):
        #Save originals for exception messages
        orig_entity = entity
        orig_value = value

        # Normalize the entity and value.
        if isinstance(entity, Entity):
            entity = self.get_peer(entity)
            if entity is None:
                raise EntityNotFoundError("%s" % orig_entity)
            assert(entity.environment == self)

        # Kinda weird special case. See EnumSetField and nnps_set
        if attribute.enum and isinstance(value, list):
            setattr(entity, '_%s' % attribute.name, value)
            return

        if isinstance(value, Entity):
            value = self.get_peer(value)
            if value is None:
                raise EntityNotFoundError("%s" % orig_value)
            assert(value.environment == self)
        if attribute.is_set_like:
            if isinstance(value, SetHandle):
                value = value._get_value()
            # You can only 'set' with a MemorySet because its value is entirely
            # known. IncompleteSet is not a candidate for a 'set' operation.
            assert isinstance(value, (list, MemorySet))
            if attribute.enum:
                value = [v for v in value]
            else:
                value = [self.get_peer(v) for v in value]

        #for whatever reason, setting the Many side of a Many-To-One
        # to None does not take unless it is read first
        # Other conditionals are added to only read the current value when
        # absolutely necessary
        if (value is None and
            attribute.related_attribute and
            not attribute.is_set_like):
                getattr(entity, attribute.name)
        setattr(entity, attribute.name, value)

    def increment(self, entity, attribute, value):
        table_name = attribute.sql_table_name
        table = elixir.metadata.tables[table_name]
        #table = self.mapper.local_table
        key_attr = entity.entity_class.key_attribute_name
        sqa_table_key_attr = getattr(table.c, key_attr)
        sqa_table_attr = getattr(table.c, attribute.name)
        from backend.core import get_session
        from sqlalchemy import update
        result = table.update(values={sqa_table_attr: sqa_table_attr + value}).where(sqa_table_key_attr==entity.key).where(sqa_table_attr!=None).execute()
        if result.rowcount == 0:
            # This is due to the row not existing or for the existing
            # value to be a None.  Try saving again with the default plus
            # the increment.
            if attribute.default is None:
                new_value = value
            else:
                new_value = attribute.default + value
            result = table.update(values={sqa_table_attr: new_value}).where(sqa_table_key_attr==entity.key).execute()

            if result.rowcount == 0:
                logger.debug("Rowcount 0, raising EntityNotFound")
                raise EntityNotFoundError("%s - %s" % (table_name, entity.key))

    @classmethod
    def load(cls, entity):
        return cls()

    def fetch(self, entity_class, key=None, has_temporary_key=True, write_only=False):
        entity = self._get_from_persists(entity_class, key)
        if entity is None and not has_temporary_key:
            entity = entity_class.persistent_class.get_by(**{entity_class.key_attribute_name:key})
            if entity is not None:
                self._add_to_persists(entity)
        if entity is None:
            assert not write_only
            logger.debug("EnvironmentPersistent -- creating Persistent object in Fetch %s %s" % (entity_class.__name__, key))
            entity = entity_class.persistent_class()
            assert entity is not None
            setattr(entity, entity_class.key_attribute_name, key)
            self._add_to_persists(entity)
        return entity

    @classmethod
    def delete(self, obj):
        o = obj.persistent_class.get_by(**{obj.entity_class.key_attribute_name:obj.key})
        if o is not None:
            from sqlalchemy.orm import object_session
            object_session(o).delete(o)

    @staticmethod
    def get_serializable_update(session, hints={}):
        if not session.deltas:
            return None
        from backend.environments.deltas import AppendOneToMany, \
            CreateEntity, DiscardOneToMany, IncrementAttribute, SetAttribute
        # If an entity is created, compress out its increments.
        createds = {}  # Map created entity to int-altering deltas.
        all_deltas = []
        filter_out = set()
        for d in session.deltas:
            if d.attribute and isinstance(d, AppendOneToMany) and \
            d.attribute.related_attribute.entity_class.persistent_class.saves_to_postgresql and \
            not d.attribute.entity_class.persistent_class.saves_to_postgresql:
                # Even though the entity may not save to postgresql,
                # the related-entity being 'appended' does not.
                # That append is really a set on the postgresql entity,
                # so replace this delta with that.
                d = SetAttribute(d.value,
                                 d.attribute.related_attribute,
                                 d.entity)

            if d.attribute and isinstance(d, DiscardOneToMany) and \
            d.attribute.related_attribute.entity_class.persistent_class.saves_to_postgresql and \
            not d.attribute.entity_class.persistent_class.saves_to_postgresql:
                # Even though the entity may not save to cassandra,
                # the related-entity being 'appended' does.  That append
                # is really a set on the cassandra entity, so replace this
                # delta with that.
                d = SetAttribute(d.value,
                                 d.attribute.related_attribute,
                                 None)
            all_deltas.append(d)
            if not d.entity.persistent_class.saves_to_postgresql or \
                (d.attribute and \
                 isinstance(d, AppendOneToMany) and \
                not d.attribute.related_attribute.entity_class.persistent_class.saves_to_postgresql) or \
                (d.attribute and isinstance(d, DiscardOneToMany) and \
                 not d.attribute.related_attribute.entity_class.persistent_class.saves_to_postgresql):
                # But otherwise filter out any deltas for entities that
                # don't save to postgresql.
                filter_out.add(d)
                continue
            if isinstance(d, CreateEntity):
                createds[d.entity] = {}
            elif isinstance(d, SetAttribute) and d.entity in createds and d.attribute.value_class == int:
                try:
                    filter_out.add(createds[d.entity][d.attribute.name])
                except KeyError:
                    pass
                createds[d.entity][d.attribute.name] = d
            elif isinstance(d, IncrementAttribute) and d.entity in createds:
                # If this is an increment and the entity is newly created.
                try:
                    cur_delta = createds[d.entity][d.attribute.name]
                except KeyError:
                    cur_delta = None
                if cur_delta:
                    cur_value = cur_delta.value
                else:
                    cur_value = 0
                new_delta = SetAttribute(d.entity,
                                         d.attribute,
                                         cur_value + d.value)
                createds[d.entity][d.attribute.name] = new_delta
                if cur_delta:
                    filter_out.add(cur_delta)
                filter_out.add(d)
                all_deltas.append(new_delta)
        deltas = [d for d in all_deltas if d not in filter_out]
        serialized_deltas = [d.serialize_sql() for d in deltas]
        accel = 'accelerated_save' in hints and hints['accelerated_save']
        return (MainPersistent.storage_id, [serialized_deltas, accel], {},)

    def run_serialized_update(self, deltas, accelerated_save):
        # NOTE: You must use backend.core.remove_session, or else testing
        # breaks. Testing overrides these methods.
        import backend.core
        try:
            start_time = now()
            operation_count = self._begin_update(deltas, accelerated_save)
            if operation_count:
                backend.core.commit_session()
                # Prepare a timed log statement.
                logger.debug("MainPersistent made %s in %s" % (
                   pluralize(operation_count, 'change'),
                   stringify(now() - start_time)))
        except Exception:
            backend.core.close_session()
            raise
        finally:
            backend.core.remove_session()

    def update(self, session, hints={}):
        """Update this persistent storage with the given session.

        Returns the number of operations performed.

        """
        serialized_deltas = EnvironmentPersistent.get_serializable_update(
                                                                session, hints)
        if not serialized_deltas:
            logger.debug("nothing to save in MainPersistent")
            return 0
        import backend.core
        try:
            start_time = now()
            operation_count = self._begin_update(*serialized_deltas[1],
                                                 **serialized_deltas[2])
            if operation_count:
                backend.core.commit_session()
                # Prepare a timed log statement.
                logger.debug("MainPersistent made %s in %s" % (
                   pluralize(operation_count, 'change'),
                   stringify(now() - start_time)))
        except Exception:
            backend.core.close_session()
            raise
        finally:
            backend.core.remove_session()

    def _begin_update(self, deltas, accelerated_save):
        """Begin update of the persistent storage with the given session."""
        if not deltas:
            logger.debug("nothing to save in MainPersistent")
            return 0

        # There are two ways to process this, one is fast, but limited.
        # If the fast way fails, the slow way is used.
        if accelerated_save:
            try:
                logger.debug("%s accelerated save %s deltas" % (
                    self, len(deltas)))
                operation_count = self._do_accelerated_update(deltas)
            except NotAccelerable:
                logger.debug("%s non-accelerated save %s deltas" % (
                    self, len(deltas)))
                operation_count = self._do_orm_update(deltas)
        else:
            operation_count = self._do_orm_update(deltas)
        return operation_count

    def _complete_update(self):
        start_time = now()
        logger.debug("MainPersistent commiting to SQL.")
        # NOTE: You must use backend.core.close_session etc b/c test overrides.
        import backend.core
        try:
            backend.core.commit_session()
        except:
            backend.core.close_session()
            raise
        finally:
            self._reset_persists()
            backend.core.remove_session()
        logger.debug("SQL commit took %s" % stringify(now() - start_time))

    def _cancel_update(self):
        self._reset_persists()
        # Note, use backend.core.remove_session b/c testing overrides.
        import backend.core
        backend.core.remove_session()

    @retry_operational_error()
    def _do_orm_update(self, serialized_deltas):
        applied_count = 0
        from backend.core import _name_to_entity_class
        from backend.environments.deltas import (TYPE_ID_TO_DELTA_TYPE,
            CreateEntity,
            DeleteEntity, SetAttribute, AppendOneToMany, AppendManyToMany,
            DiscardOneToMany, DiscardManyToMany, IncrementAttribute)

        for d in serialized_deltas:
            applied_count += 1
            delta_type_id = d[0]
            entity_data = d[1]
            entity_class_name, entity_key, entity_write_only, entity_created, entity_sql_key = entity_data
            entity_class = _name_to_entity_class[entity_class_name]
            if delta_type_id == CreateEntity.type_id:
                self.create(entity_class, entity_key)
                continue
            elif delta_type_id ==  DeleteEntity.type_id:
                entity_data = d[1]
                entity_class_name, entity_key, entity_write_only, entity_created, entity_sql_key = entity_data
                entity_class = _name_to_entity_class[entity_class_name]
                o = entity_class.persistent_class.get_by(**{entity_class.key_attribute_name:entity_key})
                if o is None:
                    raise EntityNotFoundError("%s(%s)" % (entity_class.__name__, entity_key))
                else:
                    from sqlalchemy.orm import object_session
                    object_session(o).delete(o)
                continue

            entity = self.get(entity_class, entity_key)
            if entity is None:
                raise EntityNotFoundError("%s(%s)" % (entity_class.__name__, entity_key))
            attribute_name = d[2]
            attribute = entity_class.get_attribute(attribute_name)
            if attribute.enum:
                attr_name = '_%s' % attribute.name
            else:
                attr_name = attribute.name
            value_data = d[3]

            # Increment.
            if delta_type_id == IncrementAttribute.type_id:
                self.increment(entity, attribute, value_data)
                continue

            # SetAttribute is complex because it can get None, value, enum, set
            # or enumset values.
            if delta_type_id == SetAttribute.type_id:
                if attribute.enum and isinstance(value_data, int):
                    value = attribute.enum[value_data]
                elif value_data is None:
                    value = None
                elif isinstance(value_data, tuple):
                    value_class_name, value_key, value_write_only, value_created, value_sql_key = value_data
                    value_class = _name_to_entity_class[value_class_name]
                    value = self.get(value_class, value_key)
                    if value is None:
                        raise EntityNotFoundError("%s(%s)" % (value_class.__name__, value_key))
                    assert(value.environment == self)
                elif attribute.enum:
                    assert isinstance(value_data, (list, set))
                    value = [attribute.enum[n] for n in value_data]
                elif not attribute.is_set_like:
                    value = value_data
                else:
                    assert isinstance(value_data, (list, set))
                    new_value = []
                    for v in value_data:
                        value_class_name, value_key, value_write_only, value_created, value_sql_key = v
                        value_class = _name_to_entity_class[value_class_name]
                        new_value_entity = self.get(value_class, value_key)
                        if new_value_entity is None:
                            raise EntityNotFoundError("%s(%s)" % (value_class.__name__, value_key))
                        new_value.append(new_value_entity)
                    value = new_value
                self.set_attribute(entity, attribute, value)
                continue
            # Shared variables. These deltas all take only an Entity or
            # EnumType as an argument. (no None or lists.)
            if attribute.enum:
                value = attribute.enum[value_data]
            else:
                value_class_name, value_key, value_write_only, value_created, value_sql_key = value_data
                value_class = _name_to_entity_class[value_class_name]
                value = self.get(value_class, value_key)
                if value is None:
                    raise EntityNotFoundError("%s(%s)" % (value_class.__name__, value_key))
                assert(value.environment == self)
            if delta_type_id == AppendOneToMany.type_id:
                getattr(entity, attr_name).append(value)
                continue
            elif delta_type_id == DiscardOneToMany.type_id:
                if self.attribute.enum:
                    raise NotImplementedError # This doesn't work this way.
                instrumented_list = getattr(entity, attr_name)
                assert isinstance(instrumented_list, InstrumentedList)
                instrumented_list.remove(value)
                continue
            elif delta_type_id == AppendManyToMany.type_id:
                getattr(entity, attr_name).append(value)
                continue
            elif delta_type_id == DiscardManyToMany.type_id:
                instrumented_list = getattr(entity, attr_name)
                if isinstance(instrumented_list, InstrumentedList):
                    try:
                        instrumented_list.remove(value)
                    except ValueError:
                        logger.error(
    "Attempted to remove %s from %s.%s but the value was not present in %s" % (
        value, entity, attr_name, instrumented_list))
                else:
                    logger.error("expected instrumented list, got %s" % type(instrumented_list))
                continue

        return applied_count

    @retry_operational_error()
    def _do_accelerated_update(self, serialized_deltas):
        """Save the deltas to SQL using low-level calls (no ORM).

        If the data is not savable in this manner a NotAccelerable is raised.

        This is somewhat of a hack, it is meant to speed up feed-related saves.
        If a session has an accelerated_save = True set on it the accelerated
        save will be attempted.  Any request that uses this feature needs to
        be thoroughly tested.  No guarantee that we catch every case where an
        erroneous write would safely execute.  (that is, it's not tested as
        well as the SQL Alchemy ORM.)

        these raise NotAccelerable
          deletes
          write-onlies
          setting OneToMany or ManyToMany to a list
          inherited objects

        """
        logger.debug("accelerated save %s" % pluralize(serialized_deltas,
                                                      'delta'))
        from backend.environments.deltas import (TYPE_ID_TO_DELTA_TYPE,
            CreateEntity,
            DeleteEntity, SetAttribute, AppendOneToMany, AppendManyToMany,
            DiscardOneToMany, DiscardManyToMany, IncrementAttribute)
        from backend.core import get_session, _name_to_entity_class # get SQA session.
        operation_count = 0
        inserts = {} # entity_class => set(keys)
        updates = {} # table_name => key => changed values dict
        # For increments that happen without related insert/updates.
        increments = {} # table_name => key => increment values dict
        update_table_name_to_class = {} # table_name => entity_class
        appends = {} # table_name => [left_right_value_dicts] # See AppendManyToMany

        for d in serialized_deltas:
            delta_type_id = d[0]
            entity_data = d[1]
            entity_class_name, entity_key, entity_write_only, entity_created, entity_sql_key = entity_data
            entity_class = _name_to_entity_class[entity_class_name]
            # We'll need this data for lower level work.
            persistent_class = entity_class.persistent_class
            table = persistent_class.table

            # Check for the no-attribute deltas first.
            # CreateEntity ----------------------------------------------------
            if delta_type_id == CreateEntity.type_id:
                # We don't allow inherited objects.
                if not len(persistent_class.mapper._sorted_tables) == 1:
                    logger.debug("Can't accelerate class %s, because it is a subclass" % entity_class)
                    raise NotAccelerable
                # It is a regular SQL create.
                # Add the entity's key to the inserts dict for its class.
                try:
                    keys = inserts[entity_class]
                    keys.add(entity_key)
                except KeyError:
                    keys = set([entity_key])
                    inserts[entity_class] = keys
                continue

            # DeleteEntity ----------------------------------------------------
            if delta_type_id == DeleteEntity.type_id:
                # Deletes are not accelerable.
                raise NotAccelerable

            # The attribute, if any.
            attr_name = d[2]
            attribute = entity_class.get_attribute(attr_name)

            value_data = d[3]
            # SetAttribute ----------------------------------------------------
            if delta_type_id == SetAttribute.type_id:
                # Many sections here:
                # scalar, enum, enumset, scalar and set relationships.
                # If scalar or enum...
                if not attribute.related_attribute:
                    table_name = attribute.sql_table_name
                    if table_name is None:
                        delta_type = TYPE_ID_TO_DELTA_TYPE.get(delta_type_id, delta_type_id)
                        logger.debug("Cant accel %s %s %s %s" % (delta_type, entity_class, entity_key, value_data))
                        raise NotAccelerable
                    try:
                        keys = updates[table_name]
                    except KeyError:
                        keys = {}
                        updates[table_name] = keys
                        update_table_name_to_class[table_name] = entity_class
                    try:
                        vals = keys[entity_key]
                    except KeyError:
                        vals = {}
                        keys[entity_key] = vals
                    # Scalars are encoded in their SQL form.
                    assert not isinstance(value_data, (tuple, list, set))
                    if attribute.enum and value_data is not None:
                        value_data = attribute.enum[value_data]
                    vals[attribute.name] = value_data

                    # If there is a pending increment for this entity.attribute,
                    # remove it. (set clobbers increments.)
                    # table_name => key => increment values dict
                    try:
                        by_key = increments[table_name]
                    except KeyError:
                        pass
                    else:
                        try:
                            by_attribute = increments[entity_key]
                        except KeyError:
                            pass
                        else:
                            try:
                                del by_attribute[attribute.name]
                            except KeyError:
                                pass
                    continue

                # If scalar relationship...
                if attribute.related_attribute and not attribute.is_set_like:
                    from backend.schema import OneToOne, ManyToOne, FakeManyToOne
                    assert isinstance(attribute, (
                        OneToOne, ManyToOne, FakeManyToOne))
                    if isinstance(attribute, OneToOne):
                        # In this case the uuid of the entity is kept in the
                        # table row of the value.  In certain circumstances we
                        # can't work with it.

                        # The only case we can work with is when the value is
                        # set on a newly created object.  Otherwise the write
                        # may need to clobber a potentially existing value in
                        # the existing value's table (and we can't because we
                        # don't know the value and the table.)  The solution
                        # is to write code that sets the other side of the
                        # relationship.
                        # (TODO -- Why was this only for 'None' ? I would think
                        # that any set would be a clobber. We don't make
                        # double-reverse-deltas. I'm leaving this here for that
                        # general case.)
                        if not entity_created:
                            logger.debug(
"Can't accelerate, please change code to set %s.%s=None instead of %s.%s=None" % (
attribute.related_attribute.name, attribute.name,
attribute.name, attribute.related_attribute.name))
                            raise NotAccelerable
                        # If the value is None, there is nothing to do.
                        if value_data is None:
                            continue

                    if value_data is None:
                        value_tup = None
                    else:
                        value_tup = value_class, value_key, value_write_only, value_created, value_sql_key = value_data
                    # Check for write-only
                    if value_tup and value_write_only:
                        logger.debug("Can't accelerate write only %s" % value_tup[1])
                        raise NotAccelerable

                    if isinstance(attribute, OneToOne):
                        # We are writing from the value's side.
                        key_to_add = entity_sql_key
                        table_name = attribute.related_attribute.sql_table_name
                        table_class = value_class
                        col = attribute.related_attribute.column_name
                        row = value_key
                    else:
                        assert isinstance(attribute, (ManyToOne, FakeManyToOne))
                        key_to_add = value_tup[4] if value_tup else None
                        table_name = attribute.sql_table_name
                        table_class = entity_class
                        col = attribute.column_name
                        row = entity_key
                    # These can be blank... why?
                    if table_name is None:
                        delta_type = TYPE_ID_TO_DELTA_TYPE.get(delta_type_id, delta_type_id)
                        logger.debug("Cant accel %s %s" % (delta_type, attribute))
                        logger.debug(dir(attribute))
                        raise NotAccelerable
                    try:
                        keys = updates[table_name]
                    except KeyError:
                        keys = {}
                        updates[table_name] = keys
                        update_table_name_to_class[table_name] = table_class
                    try:
                        vals = keys[row]
                    except KeyError:
                        vals = {}
                        keys[row] = vals
                    vals[col] = key_to_add
                    continue

                # If enumset...
                if attribute.enum and attribute.is_set_like:
                    if not entity_created:
                        # If entity is not created we can't be sure
                        # that we don't need to clobber old values,
                        # and if so, I don't know how.
                        logger.debug("Can't accelerate because entity is not newly created. %s" % d)
                        raise NotAccelerable
                    elif len(value_data) == 0:
                        # No-op
                        continue
                    # Put an insert for the enum set object.
                    # (entity class of EnumSet helper obj.)
                    enumset_class = attribute.value_class
                    enumset_table_name = enumset_class.persistent_class.table.name
                    if enumset_table_name is None:
                        logger.debug("Can't find table for %s" % d)
                        raise NotAccelerable
                    update_table_name_to_class[enumset_table_name] = enumset_class
                    for val in value_data:
                        key = generate_id()
                        try:
                            keys = inserts[enumset_class]
                            keys.add(key)
                        except KeyError:
                            keys = set([key])
                            inserts[enumset_class] = keys
                        # Values are kept in the 'update' section.
                        try:
                            keys = updates[enumset_table_name]
                        except KeyError:
                            keys = {}
                            updates[enumset_table_name] = keys
                        try:
                            vals = keys[key]
                        except KeyError:
                            vals = {}
                            keys[key] = vals
                        assert isinstance(val, int)
                        vals['number'] = val
                        vals[attribute.related_attribute.column_name] = entity_sql_key
                    continue
                # If set relationship...
                if attribute.related_attribute and attribute.is_set_like:
                    if not entity_created:
                        # If entity is not created we can't be sure
                        # that we don't need to clobber old values,
                        # and if so, I don't know how.
                        logger.debug("Can't accelerate because entity is not newly created. %s" % d)
                        raise NotAccelerable
                    elif len(value_data) == 0:
                        # No-op
                        continue
                    logger.debug("don't set values on related is set like")
                    raise NotAccelerable # We just don't do it otherwise.

            # IncrementAttribute ----------------------------------------------
            if delta_type_id == IncrementAttribute.type_id:
                if value_data is not None and issubclass(attribute.value_class, int):
                    table_name = attribute.sql_table_name
                    update_table_name_to_class[table_name] = entity_class
                    if table_name is None:
                        delta_type = TYPE_ID_TO_DELTA_TYPE.get(delta_type_id, delta_type_id)
                        logger.debug("Cant accel %s %s %s %s" % (delta_type, entity_class, entity_key, value_data))
                        raise NotAccelerable
                    # Increment the value in the updates structure if exists,
                    # otherwise add an increment to the increment structure.
                    # table_name => key => increment values dict
                    updated = False
                    try:
                        keys = updates[table_name]
                    except KeyError:
                        pass
                    else:
                        try:
                            vals = keys[entity_key]
                        except KeyError:
                            pass
                        else:
                            try:
                                vals[attribute.name] += value_data
                                updated = True
                            except KeyError:
                                pass
                    if not updated:
                        try:
                            by_key = increments[table_name]
                        except KeyError:
                            by_key = {}
                            increments[table_name] = by_key
                        try:
                            by_attribute = by_key[entity_key]
                        except KeyError:
                            by_attribute = {}
                            by_key[entity_key] = by_attribute
                        try:
                            existing_increment = by_attribute[attribute.name]
                        except KeyError:
                            existing_increment = 0
                        by_attribute[attribute.name] = existing_increment + value_data
                    continue

            if delta_type_id == AppendManyToMany.type_id:
                if attribute.enum:
                    logger.debug("Not implemented yet")
                    raise NotAccelerable
                if attribute.entity_class.__name__ == attribute.related_attribute.entity_class.__name__:
                    delta_type = TYPE_ID_TO_DELTA_TYPE.get(delta_type_id, delta_type_id)
                    logger.debug("Can't accelerate self ManyToMany (yet) %s" % delta_type)
                    raise NotAccelerable
                join_table_name = attribute.join_table_name
                column_format = attribute.column_format
                column_data = attribute._column_data
#                    {'person_uuid': 'person', 'feed_item_base_uuid': 'feed_item_base'}
                try:
                    dicts = appends[join_table_name]
                except KeyError:
                    dicts = []
                    appends[join_table_name] = dicts
                # append to dicts a dict that looks like this:
                # { 'left_column_name' : left_object_sql_key,
                #   'right_column_name' : right_object_sql_key }
                val_dict = {}
                # TODO: Can't we just operate on value_data?
                value_tup = value_data if value_data else None
                # Check for write-only
                if value_tup[2]:
                    logger.debug("Can't accelerate write only %s" % value_tup[1])
                    raise NotAccelerable
                value_class_name = value_tup[0]
                value_class = _name_to_entity_class[value_class_name]
                if not issubclass(value_class, Entity):
                    logger.debug("Can't accelerate? %s" % d)
                    raise NotAccelerable
                e_class_names = [persistent_class.table.name] + [b.table.name for b in persistent_class.__bases__ if isinstance(b, EntityMeta) and b != Persistent]
                v_class_names = [value_class.persistent_class.table.name] + [b.table.name for b in value_class.persistent_class.__bases__ if isinstance(b, EntityMeta) and b != Persistent]
                for k, table_name in column_data.iteritems():
                    if table_name in e_class_names:
                        x = entity_sql_key
                    elif table_name in v_class_names:
                        x = value_tup[4] # sql_key
                    else:
                        logger.debug('%s not in %s or %s' % (table_name, e_class_names, v_class_names))
                        logger.debug("Can't don't know why %s" % d)
                        raise NotAccelerable
                    val_dict[k] = x
                dicts.append(val_dict)
                continue
            delta_type = TYPE_ID_TO_DELTA_TYPE.get(delta_type_id, delta_type_id)
            logger.debug("not accelerable %s" % delta_type)
            raise NotAccelerable

        # We have accumulated data to run commands on SQL.
        if inserts or updates or appends or increments:
            for entity_class, insert_keys in inserts.iteritems():
                e = entity_class
                table = e.persistent_class.table
                table_name = table.name
                # TODO: This can be made a single operation on the same table.
                val_lists = {}
                for insert_key in insert_keys:
                    try:
                        # Clear pending updates.
                        keys = updates[table_name]
                        vals = keys[insert_key]
                        del keys[insert_key]
                    except KeyError:
                        vals = {}
                    try:
                        # Clear pending increments.
                        increment_keys = increments[table_name]
                        if insert_key in increment_keys:
                            del increment_keys[insert_key]
                    except KeyError:
                        pass
                    vals[entity_class.key_attribute_name] = insert_key
                    attrnames = frozenset(vals.keys())
                    try:
                        val_list = val_lists[attrnames]
                    except KeyError:
                        val_list = []
                        val_lists[attrnames] = val_list
                    val_list.append(vals)
                    logger.debug("--- INSERT %s %s ---" % (entity_class, insert_key))
                for val_list in val_lists.values():
                    try:
                        table.insert().execute(val_list)
                        operation_count += 1
                    except IntegrityError, e:
                        logger.error("Integrity error %s, inserting into table %s" % (e, table_name))
                        raise DuplicateKeyError("%s" % table_name)
            for table_name, keys in updates.iteritems():
                table = elixir.metadata.tables[table_name]
                for key, values_dict in keys.iteritems():
                    # Clear pending increments.
                    try:
                        increment_keys = increments[table_name]
                        attributes = increment_keys[key]
                        for k in values_dict.keys():
                            if k in attributes:
                                del attributes[k]
                    except KeyError:
                        pass
                    logger.debug("--- UPDATE --- %s %s %s" % (table_name, key, values_dict))
                    key_attr = update_table_name_to_class[table_name].key_attribute_name
                    result = table.update().where(getattr(table.c, key_attr)==key).execute(**values_dict)
                    if result.rowcount == 0:
                        logger.debug("Rowcount 0, raising EntityNotFound")
                        raise EntityNotFoundError("%s - %s" % (table_name, key))
                    operation_count += 1
            for table_name, value_dicts in appends.iteritems():
                table = elixir.metadata.tables[table_name]
                for value_dict in value_dicts:
                    try:
                        table.insert().execute(**value_dict)
                    except IntegrityError:
                        logger.debug("Could not insert into %s" % table_name)
                        raise EntityNotFoundError("%s - %s" % (table_name,
                                                               value_dict))
                    operation_count += 1
            for table_name, keys in increments.iteritems():
                table = elixir.metadata.tables[table_name]
                for key, by_attribute in keys.iteritems():
                    for attribute, value in by_attribute.iteritems():
                        if value:
                            try:
                                entity_class = update_table_name_to_class[table_name]
                                key_attr = entity_class.key_attribute_name
                                sqa_table_key_attr = getattr(table.c, key_attr)
                                logger.debug("attribute %s %s" % (type(attribute), attribute))
                                sqa_table_attr = getattr(table.c, attribute)
                                from backend.core import get_session
                                from sqlalchemy import update
                                result = table.update(values={sqa_table_attr: sqa_table_attr + value}).where(sqa_table_key_attr==key).where(sqa_table_attr!=None).execute()
                                if result.rowcount == 0:
                                    # Try to update the row, if it exists it will
                                    # update.
                                    default_value = entity_class.get_attribute(attribute).default
                                    if default_value is None:
                                        new_value = value
                                    else:
                                        new_value = default_value + value
                                    result = table.update(values={sqa_table_attr: new_value}).where(sqa_table_key_attr==key).execute()
                                    if result.rowcount == 0:
                                        raise EntityNotFoundError("%s - %s" % (table_name, key))
                            except IntegrityError:
                                logger.debug("Could not insert into %s" % table_name)
                                raise EntityNotFoundError("%s - %s" % (table_name,
                                                                       value_dict))
                            operation_count += 1
            logger.debug("accelerated save complete %s" % pluralize(
                                                        operation_count, 'op'))
            return operation_count


class MainPersistentMetaclass(type):
    """Connection to SQL Alchemy."""
    # This is called every time MainPersistent is instantiated.
    def __call__(cls, *args, **kwargs):
        # MainPersistent is a singleton that lives in thread local storage.
        try:
            singleton = MAIN_PERSISTENT.singleton
        except AttributeError:
            singleton = None
        if not singleton:
            import backend.environments.persistent
            singleton = super(
                backend.environments.persistent.MainPersistentMetaclass,
                cls).__call__(*args, **kwargs)
            MAIN_PERSISTENT.singleton = singleton
        return singleton

    def close(cls):
        """Set the threadlocal singleton reference to be None."""
        # MainPersistent is a singleton that lives in thread local storage.
        MAIN_PERSISTENT.singleton = None

    def __repr__(cls):
        return "MainPersistent"


class MainPersistent(EnvironmentPersistent):
    __metaclass__ = MainPersistentMetaclass
    storage_id = 0

    def save(self):
        from backend import get_active_session
        get_active_session().save_now.add(MainPersistent)

    def save_later(self):
        """Sugar to queue this environment in the session as 'save later."""
        from backend import get_active_session
        get_active_session().save_later.add(MainPersistent)

    @classmethod
    def _reset_persists(cls):
        """Call _reset_persists on the singleton, if it exists."""
        try:
            singleton = MAIN_PERSISTENT.singleton
        except AttributeError:
            singleton = None
        if singleton:
            # Calling _reset_persists on this instance leads to recursion.
            singleton.persistents = {}

    def __repr__(self):
        return "MainPersistent(%s)" % id(self)

MAIN_PERSISTENT = threading.local()


class ExistsPersistentMetaclass(type):
    def __call__(cls, *args, **kwargs):
        try:
            singleton = EXISTS_PERSISTENT.singleton
        except AttributeError:
            singleton = None
        if singleton is None:
            logger.debug("Creating ExistsPersistent singleton")
            import backend.environments.persistent
            singleton = super(backend.environments.persistent.ExistsPersistentMetaclass, cls).__call__(*args, **kwargs)
            EXISTS_PERSISTENT.singleton = singleton
        return singleton

    def close(cls):
        EXISTS_PERSISTENT.singleton = None

    def __repr__(cls):
        return "ExistsPersistent"


class ExistsPersistent(object):
    __metaclass__ = ExistsPersistentMetaclass

    def save(self):
        """Sugar to queue this environment in the session as 'save now'."""
        from backend import get_active_session
        get_active_session().save_now.add(ExistsPersistent)

    def save_later(self):
        """Sugar to queue this environment in the session as 'save later."""
        from backend import get_active_session
        get_active_session().save_later.add(ExistsPersistent)

    def update(self, session, hints={}):
        """Update this persistent storage with the given session."""
        logger.debug("ExistsPersistent save %s deltas." % len(session.deltas))
        users_to_update = set()
        users_to_create = set()
        from backend.environments.deltas import CreateEntity, SetAttribute
        from logic.users.existence import ExistenceCache
        from model.user import User
        for d in session.deltas:
            #Add new users to the Existence Cache
            if isinstance(d, CreateEntity):
                if isinstance(d.entity, User):
                    users_to_create.add(d.entity)
            #Detect facebook_id or username changes
            elif isinstance(d, SetAttribute):
                if (isinstance(d.entity, User) and
                    (d.attribute.name=='username' or d.attribute.name=='fb_id')):
                    users_to_update.add(d.entity)

        pipe = get_existence_redis().pipeline()

        # TODO: Confirm existence is loaded and queue a refresh if not.
        for user in users_to_update:
            ExistenceCache.update_existence_cache_entry(user, pipe=pipe)

        for user in users_to_create:
            ExistenceCache.create_entry(user.uuid, user.username, user.fb_id, pipe=pipe)

        pipe.execute()

    def __repr__(self):
        return "ExistsPersistent(%s)" % id(self)


EXISTS_PERSISTENT = threading.local()

# This is the connection to the Autocomplete service.
class AutocompleteDetectorMetaclass(type):
    def __call__(cls, *args, **kwargs):
        try:
            singleton = AUTOCOMPLETE_DETECTOR.singleton
        except AttributeError:
            singleton = None
        if singleton is None:
            logger.debug("Creating AutocompleteDetector singleton")
            import backend.environments.persistent
            singleton = super(backend.environments.persistent.AutocompleteDetectorMetaclass, cls).__call__(*args, **kwargs)
            AUTOCOMPLETE_DETECTOR.singleton = singleton
        return singleton

    def close(cls):
        AUTOCOMPLETE_DETECTOR.singleton = None

    def __repr__(cls):
        return "AutocompleteDetector"


AUTOCOMPLETE_CREATE = frozenset(settings.AUTOCOMPLETE_CREATE)
AUTOCOMPLETE_DELETE = frozenset(settings.AUTOCOMPLETE_DELETE)
AUTOCOMPLETE_UPDATE = {
}
for class_name, attributes in settings.AUTOCOMPLETE_UPDATE:
    AUTOCOMPLETE_UPDATE[class_name] = frozenset(attributes)

class AutocompleteDetector(object):
    __metaclass__ = AutocompleteDetectorMetaclass

    # TODO: Make these sugars a MixIn.
    def save(self):
        """Sugar to queue this environment in the session as 'save now'."""
        from backend import get_active_session
        get_active_session().save_now.add(AutocompleteDetector)

    def save_later(self):
        """Sugar to queue this environment in the session as 'save later'."""
        from backend import get_active_session
        get_active_session().save_later.add(AutocompleteDetector)

    def update(self, session, hints={}):
        """Update this persistent storage with the given session."""
        from backend.environments.deltas import (
            CreateEntity, SetAttribute, DeleteEntity, AppendOneToMany)

        logger.debug("Search Persistent save %s deltas" % len(session.deltas))
        creates = set()
        deletes = set()
        updates = {}

        for d in session.deltas:
            if isinstance(d, CreateEntity):
                if d.entity.entity_class.__name__ in AUTOCOMPLETE_CREATE:
                    creates.add(d.entity)
            elif isinstance(d, DeleteEntity):
                if d.entity.entity_class.__name__ in AUTOCOMPLETE_DELETE:
                    deletes.add(d.entity)
            elif isinstance(d, AppendOneToMany):
                try:
                    attrs = AUTOCOMPLETE_UPDATE[d.entity.entity_class.__name__]
                    if d.attribute.name in attrs:
                        try:
                            ups = updates[d.entity]
                        except KeyError:
                            ups = {}
                            updates[d.entity] = ups
                        try:
                            current_value = ups[d.attribute.name]
                        except KeyError:
                            current_value = []
                            ups[d.attribute.name] = current_value
                        current_value.append(d.value)
                except KeyError:
                    pass
            elif isinstance(d, SetAttribute):
                try:
                    attrs = AUTOCOMPLETE_UPDATE[d.entity.entity_class.__name__]
                    if d.attribute.name in attrs:
                        try:
                            ups = updates[d.entity]
                        except KeyError:
                            ups = {}
                            updates[d.entity] = ups
                        ups[d.attribute.name] = d.value
                except KeyError:
                    pass

        for entity in chain(creates, deletes):
            try:
                del updates[entity.entity_class.__name__]
            except KeyError:
                pass
        creates = creates - deletes

        import logic.autocomplete.client
        for entity in creates:
            if entity.entity_class.__name__=='User':
                logic.autocomplete.client.add_completion_user(entity)
        for entity in deletes:
            logger.debug('Autocomplete Delete not implemented')
        for entity, attr_value_dict in updates.iteritems():
            #this is potentially problematic b/c of the need
            # to have the UserCache loaded for this write.
            # Loading the UserCache is not required for updating UserCache
            # data
            if entity.entity_class.__name__=='User':
                try:
                    logic.autocomplete.client.add_completion_user(entity)
                except AttributeNotLoaded, e:
                    logger.exception('Could not update autocomplet for user %s' % e)

    def __repr__(self):
        return "AutocompleteDetector(%s)" % id(self)


AUTOCOMPLETE_DETECTOR = threading.local()

# This is the connection to the Search service.
class SearchDetectorMetaclass(type):
    def __call__(cls, *args, **kwargs):
        try:
            singleton = SEARCH_DETECTOR.singleton
        except AttributeError:
            singleton = None
        if singleton is None:
            logger.debug("Creating SearchDetector singleton")
            import backend.environments.persistent
            singleton = super(backend.environments.persistent.SearchDetectorMetaclass, cls).__call__(*args, **kwargs)
            SEARCH_DETECTOR.singleton = singleton
        return singleton

    def close(cls):
        SEARCH_DETECTOR.singleton = None

    def __repr__(cls):
        return "SearchDetector"

# Note: Also update in logic.search.client.MAPPINGs
# ??? maybe use frozenset with a list comprehension from the mapping... BUT
# this would create a backwards dependency...
SEARCH_STATUS_ATTRIBUTES = frozenset(['message', 'subject', 'subject_is_called',
                                      'comments', 'date'])
SEARCH_OPP_ATTRIBUTES = frozenset(['message', 'summary', 'subject', 'subject_is_called',
                                   'comments', 'date', 'opportunity_type',
                                   'service_description','location'])
SEARCH_PERSON_ATTRIBUTES = frozenset(['first_name','gender',
                                      'location','one_line_bio'])
SEARCH_PERSON_OBJ_ATTRIBUTES = frozenset(['alias','is_caled','imagecode',
                                          'imageaspect', 'obj_type','about_me'])
SEARCH_SKILL_ATTRIBUTES = frozenset(['name', 'description'])

SEARCH_CREATE = frozenset(settings.SEARCH_CREATE)
SEARCH_DELETE = frozenset(settings.SEARCH_DELETE)
SEARCH_UPDATE = {
}
for class_name, attributes in settings.SEARCH_UPDATE:
    SEARCH_UPDATE[class_name] = frozenset(attributes)

class SearchDetector(object):
    __metaclass__ = SearchDetectorMetaclass

    # TODO: Make these sugars a MixIn.
    def save(self):
        """Sugar to queue this environment in the session as 'save now'."""
        from backend import get_active_session
        get_active_session().save_now.add(SearchDetector)

    def save_later(self):
        """Sugar to queue this environment in the session as 'save later."""
        from backend import get_active_session
        get_active_session().save_later.add(SearchDetector)

    def update(self, session, hints={}):
        """Update this persistent storage with the given session."""
        from backend.environments.deltas import (
            CreateEntity, SetAttribute, DeleteEntity, AppendOneToMany)

        logger.debug("Search Persistent save %s deltas" % len(session.deltas))
        creates = set()
        deletes = set()
        updates = {}

        for d in session.deltas:
            if isinstance(d, CreateEntity):
                if d.entity.entity_class.__name__ in SEARCH_CREATE:
                    creates.add(d.entity)
            elif isinstance(d, DeleteEntity):
                if d.entity.entity_class.__name__ in SEARCH_DELETE:
                    deletes.add(d.entity)
            elif isinstance(d, AppendOneToMany):
                try:
                    attrs = SEARCH_UPDATE[d.entity.entity_class.__name__]
                    if d.attribute.name in attrs:
                        try:
                            ups = updates[d.entity]
                        except KeyError:
                            ups = {}
                            updates[d.entity] = ups
                        try:
                            current_value = ups[d.attribute.name]
                        except KeyError:
                            current_value = []
                            ups[d.attribute.name] = current_value
                        current_value.append(d.value)
                except KeyError:
                    pass
            elif isinstance(d, SetAttribute):
                try:
                    attrs = SEARCH_UPDATE[d.entity.entity_class.__name__]
                    if d.attribute.name in attrs:
                        try:
                            ups = updates[d.entity]
                        except KeyError:
                            ups = {}
                            updates[d.entity] = ups
                        ups[d.attribute.name] = d.value
                except KeyError:
                    pass

        for entity in chain(creates, deletes):
            try:
                del updates[entity.entity_class.__name__]
            except KeyError:
                pass
        creates = creates - deletes

        logger.error('Not Saving Search Detector: No methods for save.')
        # import logic.search.updates
        # for entity in creates:
        #     logic.search.updates.create(entity)
        # for entity in deletes:
        #     logic.search.updates.delete(entity.key)
        # for entity, attr_value_dict in updates.iteritems():
        #     logic.search.updates.update(entity, attr_value_dict)

    def __repr__(self):
        return "SearchDetector(%s)" % id(self)

SEARCH_DETECTOR = threading.local()

def _clear_updates(updates, entity_class):
    e = entity_class
    tables = [e.persistent_class.table] + [b.table for b in e.persistent_class.__bases__ if isinstance(b, EntityMeta) and b != Persistent]
    for table in tables:
        try:
            keys = updates[table.name]
            del keys[table.name]
        except KeyError:
            pass

STORAGE_ID_TO_STORAGE_CLASS = {
    0: MainPersistent,
    2: Cassandra
}
