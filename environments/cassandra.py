
import logging
import threading

from django.conf import settings

from util.cassandra import batch, Fetcher
from util.helpers import dicter, dicter_get, pluralize
from util.when import now, stringify
from backend import get_active_session
from backend.core import _name_to_entity_class
from backend.environments.deltas import AppendOneToMany, \
                                        CreateEntity, \
                                        DeleteEntity, \
                                        DiscardOneToMany, \
                                        IncrementAttribute, \
                                        SetAttribute, \
                                        TYPE_ID_TO_DELTA_TYPE
from backend.schema import OneToOne, OneToMany

logger = logging.getLogger("hsn.backend")
MAX_BATCH = settings.BACKEND_MAX_CASSANDRA_BATCH
BACKEND_WRITE_CONSISTENCY = settings.BACKEND_WRITE_CONSISTENCY

class CassandraMetaclass(type):
    """Connection to Cassandra."""
    # This is called every time Cassandra is instantiated.
    def __call__(cls, *args, **kwargs):
        # Cassandra is a singleton that lives in thread local storage.
        try:
            singleton = CASSANDRA.singleton
        except AttributeError:
            singleton = None
        if not singleton:
            import backend.environments.cassandra
            singleton = super(
                backend.environments.cassandra.CassandraMetaclass,
                cls).__call__(*args, **kwargs)
            CASSANDRA.singleton = singleton
        return singleton

    def close(cls):
        """Set the threadlocal singleton reference to be None."""
        # MainPersistent is a singleton that lives in thread local storage.
        CASSANDRA.singleton = None

    def __repr__(cls):
        return "Cassandra"


class Cassandra(object):
    """Backend storage in Cassandra."""
    __metaclass__ = CassandraMetaclass
    storage_id = 2

    def save(self):
        get_active_session().save_now.add(Cassandra)

    def save_later(self):
        """Sugar to queue this environment in the session as 'save later."""
        get_active_session().save_later.add(Cassandra)

    @staticmethod
    def get_serializable_update(session, hints={}):
        """Return a serializable representation of the session.deltas"""
        # If there are no deltas, return a None, indicating a no-op.
        if not session.deltas:
            return None

        # Reduce increments and assignments and filter non-cassandra entities.
        entities = {}  # Map entity to changing attrs.
        deletes = set()  # Set of Entities that are being deleted.
        creates = set()  # Set of Entities that are being created.
        all_deltas = []  # Keep track of all deltas, we filter this list later.
        filter_out = set()  # The set of deltas that will be filtered out.
        for d in session.deltas:
            # Here we replace certain deltas with ones that are more attuned
            # to Cassandra's perspective.
            # what if there is an appendonetomany from a cassandra to a postgres,
            # which means the manytoone is on a postgres that holds a key that
            # exists only in cassandra.  well... filter that fella out because it
            # is only held on postgres side... but we don't do that below...
            # so in the case of an appendonetomany you want to check if the
            # other side saves to cassandra.
            if d.attribute and isinstance(d, AppendOneToMany) and d.attribute.related_attribute.entity_class.persistent_class.saves_to_cassandra:
                # Even though the entity may not save to cassandra,
                # the related-entity being 'appended' does.  That append
                # is really a set on the cassandra entity, so replace this
                # delta with that.
                d = SetAttribute(d.value,
                                 d.attribute.related_attribute,
                                 d.entity)
            if d.attribute and isinstance(d, DiscardOneToMany) and d.attribute.related_attribute.entity_class.persistent_class.saves_to_cassandra:
                # Even though the entity may not save to cassandra,
                # the related-entity being 'appended' does.  That append
                # is really a set on the cassandra entity, so replace this
                # delta with that.
                d = SetAttribute(d.value,
                                 d.attribute.related_attribute,
                                 None)
            all_deltas.append(d)
            # Filter out deltas whose data would only write to postgres.
            # Here we check the other side of the relationship
            if d.attribute and isinstance(d.attribute, (OneToMany, OneToOne)):
                # Check the other side of the relationship.
                if not d.attribute.related_attribute.entity_class.persistent_class.saves_to_cassandra:
                    filter_out.add(d)
                    continue
            elif not d.entity.persistent_class.saves_to_cassandra:
                filter_out.add(d)
                continue
            if d.entity not in entities:
                entities[d.entity] = {}
            # If this is a delete we need all of the keys.
            if isinstance(d, DeleteEntity):
                deletes.add(d.entity)
            elif isinstance(d, CreateEntity):
                creates.add(d.entity)
            # If there is a previous assignment of the same attribute 
            # replace it with this more-recent one.
            elif isinstance(d, SetAttribute):
                try:
                    filter_out.add(entities[d.entity][d.attribute.name])
                except KeyError:
                    pass
                entities[d.entity][d.attribute.name] = d
            # If there is a previous increment of the same attribute compress
            # the multiple incerements into one increment.
            elif isinstance(d, IncrementAttribute):
                try:
                    cur_delta = entities[d.entity][d.attribute.name]
                    cur_value = cur_delta.value
                except KeyError:
                    cur_delta = None
                    cur_value = 0
                new_delta = IncrementAttribute(d.entity,
                                               d.attribute,
                                               cur_value + d.value)
                entities[d.entity][d.attribute.name] = new_delta
                if cur_delta:
                    filter_out.add(cur_delta)
                filter_out.add(d)
                all_deltas.append(new_delta)
            # Don't serialize deltas for empty sets that are not kept in this
            # column family.
            if isinstance(d.attribute, (OneToMany, OneToOne)) and not d.value:
                filter_out.add(d)
        # filter out any deltas that operate on deleted Entities.
        if deletes:
            for d in all_deltas:
                if d.entity in deletes:
                    if isinstance(d, DeleteEntity) and d.entity not in creates:
                        # Don't filter out the Delete if the Entity was not
                        # created in this session.
                        pass
                    else:
                        filter_out.add(d)
            for entity in deletes:
                del entities[entity]
            deletes = deletes - creates

        # Now that we've fixed up the delta list to our liking, filter out what
        # needs filtering and serialize.
        deltas = [d for d in all_deltas if d not in filter_out]
        if not deltas:
            return None

        serialized_deltas = [d.serialize_cassandra() for d in deltas]
        consistency = hints.get('consistency')

        # If these deltas will cause a composite key index table to be written
        # for the first time (INSERT, not UPDATE) then we must include all
        # of the entity data (so that it may be written to the index table.)
        # If these deltas will cause an UPDATE to a composite key table then
        # we must ensure that all key data is included in the serialization.
        # (if this data is not already included in the deltas.)
        # Remove any Entitys from the deletes that were created this session.
        keys = {}
        for entity in deletes:
            composite_keys = entity.persistent_class.indexes
            if composite_keys:
                # Keys will have to have the values of all updateable index
                # tables' keys.
                key_attr_name = entity.key_attribute_name
                write_indexes = {}
                for index_name, attr_names in composite_keys.iteritems():
                    empty_key_found = False
                    for attr_name in attr_names:
                        if attr_name != key_attr_name:
                            try:
                                found_existing_value = object.__getattribute__(entity, attr_name)
                            except:
                                found_existing_value = entity.environment.get_attribute(entity, entity.entity_class.get_attribute(attr_name), create_write_only=False)
                            if found_existing_value is None or found_existing_value == '':
                                empty_key_found = True
                                break
                    if not empty_key_found:
                        write_indexes[index_name] = attr_names
                missing_attrs = set()
                for index_name, attr_names in write_indexes.iteritems():
                    for attr_name in attr_names:
                        if attr_name != key_attr_name:
                            missing_attrs.add(attr_name)
                encoded_key = entity.get_key_attribute().cassandra_encode(entity.key)
                key_values = {}
                for missing_attr in missing_attrs:
                    attribute = entity.get_attribute(missing_attr)
                    current_value = getattr(entity, missing_attr)
                    encoded_value = attribute.cassandra_encode(current_value)
                    key_values[missing_attr] = encoded_value
                if key_values:
                    keys[encoded_key] = key_values

        for entity, changed_attrs in entities.iteritems():
            composite_keys = entity.persistent_class.indexes
            if composite_keys:
                # Note this logic is based on the assumption that certain
                # error conditions are detected at assignment time.
                # see backend.environments.core.Entity.__setattribute__

                # This entity is about to be written to.  We may need to
                # include additional information.  We need to know if any
                # of the index tables will be written to.  If they have any
                # empty key values they will not write.
                key_attr_name = entity.key_attribute_name
                write_indexes = {}
                for index_name, attr_names in composite_keys.iteritems():
                    empty_key_found = False
                    for attr_name in attr_names:
                        if attr_name != key_attr_name:
                            if attr_name in changed_attrs:
                                changed_attrs_value = changed_attrs[attr_name]
                                if changed_attrs_value is None or changed_attrs_value == '':
                                    empty_key_found = True
                                    break
                            else:
                                found_existing_value = getattr(entity, attr_name)
                                if found_existing_value is None or found_existing_value == '':
                                    empty_key_found = True
                                    break
                    if not empty_key_found:
                        write_indexes[index_name] = attr_names

                # If any of the index tables will be written for the first time
                # (INSERT instead of UPDATE) we need to include all missing
                # attributes, not just missing keys.
                has_insert_indexes = False
                update_indexes = {}
                for index_name, attr_names in write_indexes.iteritems():    
                    # If any of the keys are in the deltas it was allowed
                    # to be assigned, therefore this index did not already
                    # have a row for this entity and this is an INSERT.  If
                    # all of the keys are not in the
                    # deltas (but the index was determined to be writeable
                    # above), then we know this is an UPDATE.
                    is_update = True
                    for attr_name in attr_names:
                        if attr_name in changed_attrs:
                            has_insert_indexes = True
                            is_update = False
                            break
                    if is_update:
                        update_indexes[index_name] = attr_names

                # If there are indexes to write, put any necessary values into
                # the keys dict that are not to be found in the deltas.
                missing_attrs = set()
                # For the INSERTS put every attribute that is not in the
                # deltas into the keys.  Note that AllAttributesNotLoaded
                # would be raised if an INSERT-inducing assignment occurred
                # and all attributes were not known.
                if has_insert_indexes:
                    for attr_name, attr in entity.entity_class.attributes.iteritems():
                        # Increment attributes do not live in the index tables.
                        if type(attr).__name__ in ('Incrementer', 'OneToMany', 'OneToOne'):
                            continue
                        if attr_name != key_attr_name and attr_name not in changed_attrs:
                            missing_attrs.add(attr_name)
                # (seeing as how we just put all missing attrs into the 'keys'
                # dict we can skip checking for individual keys that we may
                # have otherwise needed to add.)
                else:
                    for index_name, attr_names in update_indexes.iteritems():
                        for attr_name in attr_names:
                            if attr_name != key_attr_name and attr_name not in changed_attrs:
                                missing_attrs.add(attr_name)
                encoded_key = entity.get_key_attribute().cassandra_encode(entity.key)
                key_values = {}
                for missing_attr in missing_attrs:
                    attribute = entity.get_attribute(missing_attr)
                    current_value = getattr(entity, missing_attr)
                    encoded_value = attribute.cassandra_encode(current_value)
                    key_values[missing_attr] = encoded_value
                if key_values:
                    keys[encoded_key] = key_values
        args = (serialized_deltas, consistency)
        if keys:
            kwargs = {'keys': keys}
        else:
            kwargs = {}
        return (Cassandra.storage_id, args, kwargs,)

    def run_serialized_update(self, *args, **kwargs):
        """The Session calls this on storage systems with a serialized update.

        By "serialized" we mean a previously encoded and decoded set of args
        and kwargs which we simply pass on to _begin_update.

        Note that this function allows the session to call an API-level
        function on this storage system (Cassandra.)  We are not using any sort
        of connection, batch, or transaction, but if we were, this would be the
        place to determine what to do with what was handed to us.  It just so
        happens that _begin_update is all we need to do, but if for some reason
        we needed to close a connection after the update, (a presumably
        expensive operation) we would normally check if any operations were
        applied.  Since we do none of that on Cassandra this code looks
        overwrought, as it follows a pattern from SQL which does have these
        issues.  (see backend.environments.persistent)

        """
        start_time = now()
        operation_count = self._begin_update(*args, **kwargs)
        if operation_count:
            # Prepare a timed log statement.
            logger.debug("Cassandra made %s in %s",
                pluralize(operation_count, 'change'),
                stringify(now() - start_time))

    def update(self, session, hints={}):
        """Update this persistent storage with the given session.

        Returns the number of operations performed.

        """
        serialized_deltas = Cassandra.get_serializable_update(session, hints)
        if not serialized_deltas:
            logger.debug("nothing to save in Cassandra")
            return 0
        start_time = now()
        operation_count = self._begin_update(*serialized_deltas[1],
                                             **serialized_deltas[2])
        if operation_count:
            # Prepare a timed log statement.
            logger.debug("Cassandra made %s in %s",
                pluralize(operation_count, 'change'),
                stringify(now() - start_time))

    def _begin_update(self, *args, **kwargs):
        """Begin update of the Cassandra storage with the given data.

        (this is just 'update', not really '_begin_update'.  If Cassandra had
        a transaction this would be what goes inside of it, but without a
        transaction, this is just a write.)

        args
        serialized_deltas -- list of serialized Deltas to apply to Cassandra
        consistency -- minimum consistency for this operation, None for default

        kwargs
        'keys' -- dict of composite-key data for each Entity created/changed
                  that has composite keys.
                  entity-key => attribute name => value
                  (note the name is vestigial.  It would make more sense to
                  call it 'attrs')

        (Note tha args and kwargs are unspecified in the signature for reasons
        of forward and backward compatibility when deploying updates that
        change the number of arguments or presence of keywords.  Any missing
        argument should have a safe default (implying it comes from an older
        serialization.  this is backward compatibility.)  Any additional
        argument should be ignored (implying we have rolled out serializers
        that now include more data/arguments than before, but that the features
        are as of yet unused (this is forward compatibility.)

        """
        # Normalize arguments.
        if len(args) > 0:
            serialized_deltas = args[0]
        else:
            serialized_deltas = []
            
        if not serialized_deltas:
            logger.debug("nothing to save in Cassandra")
            return 0

        if len(args) > 1:
            consistency = args[1]
            if consistency == 'None':
                consistency = None
        else:
            consistency = None

        extra_keys = kwargs.get('keys', {})

        # A helper function for separating commands by consistency.
        consistency_memo = {}
        def _get_consistency(entity_class_name):
            if consistency:
                return consistency
            try:
                return consistency_memo[entity_class_name]
            except KeyError:
                pass
            entity_class = _name_to_entity_class[entity_class_name]
            if entity_class.consistency is not None:
                consistency_memo[entity_class_name] = entity_class.consistency
                return entity_class.consistency
            else:
                consistency_memo[entity_class_name] = BACKEND_WRITE_CONSISTENCY
                return BACKEND_WRITE_CONSISTENCY

        # We accumulate the deltas into these structures so that we can issue
        # the correct commands to the util.cassandra write functions.
        # All redundancy has already been removed by the serializer.
        operation_count = 0
        # consistency => opname => entity_class_name =>
        #     if opname 'update' or 'increment', key => attrname => value
        #     if opname 'delete', set(keys)
        by_consistency = {}
        # 'creates' dict is used so that we don't issue SetAttribute commands
        # for attributes whose value is None in the case of a Create.
        # When util.cassandra.batch gets a None it issues a column delete,
        # which is redundant in the case of a newly inserted row.
        # Also we need 'creates' so we can add the hsn_deleted=False.
        # entity_class_name => key
        creates = {}
        # We keep track of attrs on creates that have been set to None
        # so that we can write them to indexes that are getting written for
        # the first time.
        create_set_nones = {}

        for d in serialized_deltas:
            # (1, ('Foo', '02591f50-41e3-480f-9159-28ab4aa9169f'))
            delta_type_id = d[0]
            entity_data = d[1]
            entity_class_name, key = entity_data
            entity_class = _name_to_entity_class[entity_class_name]
            
            # Check for the no-attribute deltas first.
            # CreateEntity ----------------------------------------------------
            if delta_type_id == CreateEntity.type_id:
                key_attribute_name = entity_class.key_attribute_name
                this_consistency = _get_consistency(entity_class_name)
                dicter(by_consistency,
                       [this_consistency,
                        'update',
                        entity_class_name,
                        key,
                        entity_class.key_attribute_name],
                       key)
                try:
                    keys = creates[entity_class_name]
                    keys.add(key)
                except KeyError:
                    keys = set([key])
                    creates[entity_class_name] = keys
                continue

            # DeleteEntity ----------------------------------------------------
            if delta_type_id == DeleteEntity.type_id:
                this_consistency = _get_consistency(entity_class_name)
                by_entity_class_name = dicter_get(by_consistency,
                                                  [this_consistency, 'delete'])
                try:
                    keys = by_entity_class_name[entity_class_name]
                    keys.add(key)
                except KeyError:
                    keys = set([key])
                    by_entity_class_name[entity_class_name] = keys
                continue

            # The attribute, if any.
            attr_name = d[2]
            attribute = entity_class.get_attribute(attr_name)
            value_data = d[3]
            
            # SetAttribute ----------------------------------------------------
            if delta_type_id == SetAttribute.type_id:
                # Note: increment fields will not have SetAttribute deltas.
                # Note: SetAttribute will only exist for the side of
                # the relationship that holds the key: FakeManyToOne, ManyToOne
                # or their cassandra-only variants.

                # If value is None and this is a create it's a no-op on
                # write because a missing column in Cassandra is interpretted
                # as a None.  However we need to know if we set this to None
                # (as opposed to doing nothing) so we at least keep track of
                # it in the set_nones dict.
                if value_data is None:
                    try:
                        if key in creates[entity_class_name]:
                            dicter(create_set_nones,
                                   [entity_class_name, key, attr_name],
                                   None)
                            continue
                    except KeyError:
                        pass
                this_consistency = _get_consistency(entity_class_name)
                dicter(by_consistency,
                       [this_consistency,
                        'update',
                        entity_class_name,
                        key,
                        attr_name],
                       value_data)
                continue

            # IncrementAttribute ----------------------------------------------
            if delta_type_id == IncrementAttribute.type_id:
                # Note that the serializer compresses all increments into a
                # single increment so we need only set the value in our
                # increments.  We don't need to check for it first.
                this_consistency = _get_consistency(entity_class_name)
                dicter(by_consistency,
                       [this_consistency,
                        'increment',
                        entity_class_name,
                        key,
                        attr_name],
                       value_data)
                continue

            delta_type = TYPE_ID_TO_DELTA_TYPE.get(delta_type_id, delta_type_id)
            logger.warn("%s not supported in Cassandra, ignoring", delta_type)
            continue

        # -- Call util.cassandra.batch ----------------------------------------

        # Convert these datastructes to calls to util.cassandra.batch
        for consistency_level, by_opcode in by_consistency.iteritems():
            # column_family -> attr_name -> value
            # all keys must be in the attr_name -> value dict.
            batch_cfs_data_pairs = {}
            batch_increments = {}
            batch_deletes = {}
            
            updates = by_opcode.get('update', {})
            for entity_class_name, by_key in updates.iteritems():
                entity_class = _name_to_entity_class[entity_class_name]
                key_attribute_name = entity_class.key_attribute_name
                for primary_key, by_attr in by_key.iteritems():
                    values_dict = by_attr.copy()
                    if primary_key in creates.get(entity_class_name, {}):
                        values_dict['hsn_deleted'] = False
                    # we construct a values dict per column family with
                    # the updated attrs and the keys.
                    column_family = entity_class.get_primary_column_family()
                    values_dict[entity_class.key_attribute_name] = primary_key
                    try:
                        dict_list = batch_cfs_data_pairs[(column_family, (key_attribute_name,),)]
                    except:
                        dict_list = []
                        batch_cfs_data_pairs[(column_family, (key_attribute_name,),)] = dict_list
                    dict_list.append(values_dict)
                    these_extra_keys = extra_keys.get(primary_key, {})
                    # make updates for the composite key index column families.
                    indexes = entity_class.persistent_class.indexes
                    for index, index_key_names in indexes.iteritems():
                        # First see if this index will see any activity.
                        # If all key values are not present, there is no
                        # write.
                        # If a key's value is being set (and all keys
                        # are present) this is an INSERT
                        # Otherwise this is an UPDATE.  (there is no difference
                        # to cassandra as everything is an UPDATE, but for our
                        # logic we need to know if we just supply the changed
                        # attributes or all of the attributes.)
                        #----------
                        index_has_sufficient_keys = True
                        index_key_values = {}
                        for index_key_name in index_key_names:
                            if index_key_name == key_attribute_name:
                                index_value = primary_key
                            else:
                                try:
                                    index_value = by_attr[index_key_name]
                                except KeyError:
                                    try:
                                        index_value = these_extra_keys[index_key_name]
                                    except KeyError:
                                        index_value = None
                                    if index_value is None or index_value == '':
                                        index_has_sufficient_keys = False
                                        logger.debug('not writing to %s(%s) %s because value for %s is None',
                                                     entity_class_name,
                                                     str(primary_key),
                                                     index,
                                                     index_key_name)
                                        break
                            index_key_values[index_key_name] = index_value
                        if index_has_sufficient_keys:
                            column_family = entity_class.get_index_column_family(index)
                            values_dict = by_attr.copy()
                            values_dict.update(index_key_values)
                            index_key_names_set = set(index_key_names)
                            key_assigned = False
                            # If any of the values in by_attr are a key value
                            # that means the key was set.  Since one is
                            # disallowed from setting an index key's value
                            # after it has been set, this means we are writing
                            # to this index for the first time, and we must
                            # write the entire row.
                            for attr_name in by_attr.keys():
                                if attr_name != key_attribute_name and \
                                   attr_name in index_key_names_set:
                                    key_assigned = True
                                    break
                            if key_assigned:
                                values_dict['hsn_deleted'] = False
                                # copy in to the values_dict all missing
                                # attributes, getting their values from
                                # the these_extra_keys dict.
                                for attr_name, attribute in entity_class.attributes.iteritems():
                                    if attr_name not in values_dict:
                                        if attr_name == key_attribute_name:
                                            values_dict[attr_name] = primary_key
                                        elif type(attribute).__name__ not in ('Incrementer', 'OneToMany', 'OneToOne'):
                                            try:
                                                values_dict[attr_name] = these_extra_keys[attr_name]
                                            except KeyError:
                                                values_dict[attr_name] = create_set_nones[entity_class_name][primary_key][attr_name]
                            try:
                                dict_list = batch_cfs_data_pairs[(column_family, index_key_names,)]
                            except KeyError:
                                dict_list = []
                                batch_cfs_data_pairs[(column_family, index_key_names,)] = dict_list
                            dict_list.append(values_dict)

            increments = by_opcode.get('increment', {})
            for entity_class_name, by_key in increments.iteritems():
                entity_class = _name_to_entity_class[entity_class_name]
                # Increments are expecting this:
                # (column_family, pk) -> values
                # values -> [((name, value),(name2,value2)), ((name, value3),(name2,value4))] or values -> [{name: value, name2: value2},{name:value3, name2: value4}]
                column_family = entity_class.get_counter_column_family()
                cf_arg = (column_family, (entity_class.key_attribute_name,))
                for key, by_attr in by_key.iteritems():
                    by_attr[entity_class.key_attribute_name] = key
                    try:
                        dict_list = batch_increments[cf_arg]
                    except KeyError:
                        dict_list = []
                        batch_increments[cf_arg] = dict_list
                    dict_list.append(by_attr)

            deletes = by_opcode.get('delete', {})
            for entity_class_name, keys in deletes.iteritems():
                entity_class = _name_to_entity_class[entity_class_name]
                column_family = entity_class.get_primary_column_family()
                key_attribute_name = entity_class.key_attribute_name
                values = []
                for key in keys:
                    values.append({key_attribute_name: key})
                batch_deletes[column_family] = values

                indexes = entity_class.persistent_class.indexes
                for index, key_names in indexes.iteritems():
                    column_family = entity_class.get_index_column_family(index)
                    values = []
                    for key in keys:
                        these_extra_keys = extra_keys.get(key, {})
                        values_dict = {}
                        for key_name in key_names:
                            if key_name == key_attribute_name:
                                values_dict[key_name] = key
                            elif key_name not in values_dict:
                                values_dict[key_name] = these_extra_keys[key_name]
                        values.append(values_dict)
                    batch_deletes[column_family] = values

                has_incr = False
                for attribute in entity_class.attributes_order:
                    if type(attribute).__name__ == 'Incrementer':
                        has_incr = True
                        break
                if has_incr:
                    column_family = entity_class.get_counter_column_family()
                    key_attribute_name = entity_class.key_attribute_name
                    values = []
                    for key in keys:
                        values.append({key_attribute_name: key})
                    batch_deletes[column_family] = values

            batch(cfs_data_pairs=batch_cfs_data_pairs,
                  increments=batch_increments,
                  deletes=batch_deletes,
                  consistency_level=consistency_level)
            operation_count += 1

        logger.debug("Cassandra save complete %s",
                     pluralize(operation_count, 'op'))
        return operation_count

    def _complete_update(self):
        raise NotImplementedError

    def _cancel_update(self):
        raise NotImplementedError

    @staticmethod
    def exists(entity_class, entity_key, consistency=None):
        """True if entity's key is in entity's Cassandra column family."""
        if consistency is not None:
            this_consistency = consistency
        elif entity_class.consistency is not None:
            this_consistency = entity_class.consistency
        else:
            this_consistency = settings.BACKEND_READ_CONSISTENCY
        key_attr = entity_class.key_attribute_name
        column_family = entity_class.get_primary_column_family()
        fetcher = Fetcher(cf_name=column_family, fields=(key_attr,), consistency_level=this_consistency)
        fetcher.add_column_value_relation(key_attr, entity_key)
        result = None
        try:
            result = fetcher.fetch_first()
            if result is None:
                logger.info('Exists check %s did not find a record', fetcher)
        except Exception, e:
            logger.error('Cassandra.exists raised %s', e)
            logger.exception('Cassandra.exists encountered an error with query: %s', fetcher)
            raise

        return result is not None

    def __repr__(self):
        return "Cassandra(%s)" % id(self)


CASSANDRA = threading.local()
