
import logging
from uuid import UUID

from sqlalchemy.orm.collections import InstrumentedList

from util.deprecate import deprecated
from backend.core import _name_to_entity_class
from backend.datastructures import MemorySet, IncompleteSet, patch
from backend.exceptions import AttributeNotLoaded
from backend.schema import EnumValue, \
                           FakeManyToOne, \
                           ManyToOne, \
                           OneToMany, \
                           OneToOne, \
                           Persistent
from backend.environments.core import Environment, Entity, EntityMetaclass
from backend.schema import Attribute


logger = logging.getLogger("hsn.backend.memory")


class _EnvironmentMemory(Environment):
    """
    Implements an unique-entity system. Intended as a subclass for any
    Environment that does not have its own unique-entity system.

    Note: Every Environment except EnvironmentPersistent
    is based on _EnvironmentMemory. (EnvironmentPersistent is Elixir, which has
    its own Entities (Persistent subclasses) and memory-space (a connection
    to an SQL Alchemy database.)

    """

    # True if inventory rules apply to this environment.
    inventoried = False
    # An inventory of None means 'autoload'.
    inventory = None
    # Subclasses must set this to be the class of Obj this cache is keyed on.
    entity_class = None
    patch_inventory = None # Inventory of data to patch on load.

    def __init__(self, key=None):
        self.instances_by_class = {}
        self.values = {} # [entity_class][key][attribute_name] = value
        self.list_handles = {} # [entity_class][attribute_name] = value
        if self.inventoried:
            assert not key is None
            value_class =  self.entity_class.get_attribute(
                             self.entity_class.key_attribute_name).value_class
#            if not isinstance(key, value_class):
            if value_class == UUID:
                types_match = type(key).__name__ in ['UUID', 'FastUUID']
            else:
                types_match = isinstance(key, value_class)
            if not types_match:
                raise TypeError(
                    '%s expected type %s but instead got %s of type %s ' %(
                        self,
                        value_class,
                        key,
                        type(key)))
            self.entity_key = key
        super(_EnvironmentMemory, self).__init__()

    def get_attribute(self, given_entity, attribute):
        """Get this Environment's value for this attribute for this Entity.

        If return is an Entity or a SetHandle it is local to this environment.

        """
        assert not given_entity._deleted
        entity = self.get_peer(given_entity)
        if entity is None:
            raise AttributeNotLoaded("Entity is None")
        assert entity.environment == self
        found = self._memory_get_value(entity, attribute,
            generate_incomplete_list=True)
        if attribute.is_set_like:
            found = SetHandle(entity, attribute)
        return found

    def set_attribute(self, entity, attribute, value):
        self._memory_set_attribute(entity, attribute, value)

    # This is separated out so that you may effect your mixin memory without
    # having to call super methods.
    def _memory_set_attribute(self, entity, attribute, value):
        """Supports high-level operations of setting an attribute in memory.

        When a value is set reverse-values are also adjusted.

        Peer entities and peer values are welcome.  All input is localized.
        
        """
        # If this Entity is marked for deletion we shouldn't set values on it.
        assert not entity._deleted, u'Attempted set on deleted %s' % entity

        # attribute.related_attribute is None unless the attribute is a
        # relationship (ManyToMany, OneToOne...) in which case it is the
        # attribute that represents the other side of the relationship.
        related_attribute = attribute.related_attribute
        
        # Get a local peer of the given entity.
        entity = self.fetch_peer(entity)
        
        # If this set is on a relationship, clear old reverse values, if any.
        # (Why? Because this is a 'set' and not an append or remove. This means
        # that all old values are cleared and all new values set.)
        if related_attribute and attribute.enum is None:
            # See if there is an existing value for this relationship.
            old_value = None
            try:
                # Note that without the keyword argument an incomplete list
                # of zero items would be put in memory for this attribute.
                old_value = self._memory_get_value(entity, attribute,
                                                generate_incomplete_list=False)
            except AttributeNotLoaded:
                pass
            if old_value is not None:
                # TODO: Would this be a good place to "redirect write-onlies" ?
                # if the old value is a write-only... could it be referring to
                # this value? I suppose not because this is a "set attribute"
                # but what if this were a "promote attribitue", that might
                # allow for better handling.
                if attribute.is_set_like:
                    #logger.debug("__mem__ list like")
                    if isinstance(old_value, IncompleteSet):
                        # If the old value is an incomplete set, clear the
                        # 'contains'.
                        values_to_clear = old_value._contains
                    else:
                        assert isinstance(old_value, MemorySet)
                        values_to_clear = old_value

                    # If the related_attribute is list like, it means that this
                    # entity appears in that list.
                    # otherwise, it means that this entity is the related value.
                    for o_v in values_to_clear:
                        if o_v.environment != self:
                            o_v = self.fetch_peer(o_v)
                        assert(o_v.environment == self)
                        # Get the reverse value.
                        reverse_value = None
                        try:
                            reverse_value = self._memory_get_value(o_v, related_attribute, generate_incomplete_list=False)
                        except AttributeNotLoaded:
                            pass
                        if reverse_value is not None:
                            if related_attribute.is_set_like:
                                assert(isinstance(reverse_value, (MemorySet, IncompleteSet)))
#                                assert(entity in reverse_value)
                                reverse_value.discard(entity)
                                # Track modification if this is a Session.
                                from backend.environments.session import Session
                                if isinstance(self, Session):
                                    reverse_value._modifiers.add(self)
                            else:
                                self._memory_set_value(reverse_value,
                                                       related_attribute,
                                                       None)
                else:
                    if related_attribute.is_set_like:
                        assert isinstance(old_value, Entity)
                        assert(old_value.environment == self)
                        # Get the reverse value. (If this value exists, so should
                        # its.)
                        reverse_value = None
                        try:
                            reverse_value = self._memory_get_value(old_value, related_attribute, generate_incomplete_list=False)
                        except AttributeNotLoaded:
                            pass
                        if reverse_value is not None:
                            assert isinstance(reverse_value, (list, IncompleteSet, MemorySet))
#                            assert entity in reverse_value
                            reverse_value.discard(entity)
                            from backend.environments.session import Session
                            if isinstance(self, Session):
                                reverse_value._modifiers.add(self)
                    else:
                        # Otherwise, just set the value to None.
                        assert(old_value.environment == self)
                        # This is a tentative use of this special case, it could
                        # be spread around to the other types of relationship
                        # but for now it is meant to be used for obj-sub-obj
                        # relationships.
                        needs_setting = True
                        if old_value.write_only:
                            from backend.environments.session import Session
                            # Only sessions have write onlies.
                            assert isinstance(old_value.environment, Session)
                            if self._adjust_if_write_only_connector(entity,
                                                                    attribute,
                                                                    value):
                                needs_setting = False
                        if needs_setting:
                            # If we have not substituted, then this would set
                            # the relationship to None.
                            self._memory_set_value(old_value,
                                                   related_attribute,
                                                   None)
#        logger.debug('__mem__  set checking asserts before setting value: %s' % value)
#        assert(not isinstance(value, Persistent))
        # If the value we've been given is an SQL object, make a local peer
        # of it now.
        if isinstance(value, (Entity, Persistent)):
            value = self.fetch_peer(value)

        if __debug__: # ??? remove the below?
            if attribute.enum:
                if attribute.is_set_like:
                    if not isinstance(value, IncompleteSet):
                        for v in value:
                            assert isinstance(v, EnumValue)
                else:
                    assert (value is None) or isinstance(value, EnumValue), "%s value is %s" % (attribute.name, type(value))

        # If the value is a SetHandle, localize it.
#        logger.debug("__mem__ checking for list handle: %s" % value)
        if isinstance(value, SetHandle):
#            logger.debug("__mem__...is handle")
            assert attribute.is_set_like
#            logger.debug("__mem__...setting value")
            value = value._make_local_copy_of_value(self)
            from backend.environments.session import Session
            if isinstance(self, Session):
                value._modifiers.add(self)
#            logger.debug("__mem__...set it to be: %s" % value)
            assert not isinstance(value, SetHandle)
#            logger.debug("__mem__...handle done: %s" % value)
        elif attribute.is_set_like:
#            logger.debug('value -- %s' % value)
#            logger.debug("__mem__... is not handle")
            if isinstance(value, IncompleteSet):
                if attribute.enum:
                    l = IncompleteSet(set(list(value._contains)),
                                      set(list(value._does_not_contain)),
                                      value._size)
                else:
                    l = IncompleteSet(
                        set([self.fetch_peer(v) for v in value._contains]),
                        set([self.fetch_peer(v) for v in value._does_not_contain]),
                        value._size)
            elif isinstance(value, (InstrumentedList, list, MemorySet)):
                # If it isnt a list handle it can still be a list, so localize it.
                l = MemorySet()
                for v in value:
                    if attribute.enum:
                        l.add(v)
                    else:
                        l.add(self.fetch_peer(v))
            else:
                raise NotImplementedError
            value = l
            from backend.environments.session import Session
            if isinstance(self, Session):
                value._modifiers.add(self)
#            logger.debug("__mem__ ... is not handle done.")

#        logger.debug("__mem__ setting value: %s.%s=%s" % (entity, attribute.name, value))
        self._memory_set_value(entity, attribute, value)

        if isinstance(value, SetHandle):
            valid_value = value._get_value()
        else:
            valid_value = value
        if isinstance(valid_value, IncompleteSet):
            valid_value = valid_value._contains
        else:
            valid_value = value
        # For relationships, set the reverse value.
        if valid_value and related_attribute and not attribute.enum:
 #           logger.debug("__mem__ setting reverse value for:\n %s.%s = %s" % (entity, attribute.name, value))

            # There are 4 cases here.
            # 1: Source = Single, Other = Single
            # 2: Source = List, Other = Single
            # 3: Source = Single, Other = List
            # 4: Source = List, Other = List

            # 1: Source = Single, Other = Single
            # Just set the value's related attribute to be this entity.
            if not attribute.is_set_like and not related_attribute.is_set_like:
                assert(isinstance(value, Entity))
                assert(value.environment == self)
#                logger.debug("__mem__ setting...")
                self._memory_set_value(value, related_attribute, entity)
            # 2: Source = List, Other = Single
            # For each object in the value, set its related attribute to be
            # this entity.
            if attribute.is_set_like and not related_attribute.is_set_like:
                if isinstance(value, IncompleteSet):
                    _value = value._contains
                else:
                    _value = value
                for v in _value:
                    #assert(v.environment == self)
                    self._memory_set_value(v, related_attribute, entity)
            # 3: Source = Single, Other = List
            # Find the value's related attribute (a list) and add this entity
            # to it.
            if not attribute.is_set_like and related_attribute.is_set_like:
                # In this case get the other's value and add this to it.
                assert(isinstance(value, Entity))
                related_value = None
                try:
                    related_value = self._memory_get_value(value, related_attribute)
                except AttributeNotLoaded:
                    related_value = IncompleteSet()
                    self._memory_set_value(value, related_attribute, related_value)
#                logger.debug("__mem__ related value is: %s" % related_value)
                assert isinstance(related_value, (IncompleteSet, MemorySet))
                related_value.add(entity)
#                logger.debug("__mem__ related value now is: %s" % related_value)
                from backend.environments.session import Session
                if isinstance(self, Session):
                    related_value._modifiers.add(self)
            # 4: Source = List, Other = List
            # For each object in value (a list), get that object's related
            # value (a list) and add the entity to it.
#            logger.debug("checking value: %s" % value)
            if attribute.is_set_like and related_attribute.is_set_like:
                # In this case get each of the value's related_value lists
                # and add this to it.
#                assert(isinstance(value, (list, IncompleteSet)))
                if isinstance(value, IncompleteSet):
                    contains = value._contains
                    does_not_contain = value._does_not_contain
                else:
                    contains = value
                    does_not_contain = []

                for v in contains:
#                    logger.debug("v in contains is %s, attr is %s" % (v, related_attribute.name))
                    assert(v.environment == self)
                    related_value = None
                    try:
                        related_value = self._memory_get_value(v, related_attribute)
                    except AttributeNotLoaded:
                        related_value = IncompleteSet()
                        self._memory_set_value(v, related_attribute, related_value)
                    assert isinstance(related_value, (MemorySet, IncompleteSet))
                    related_value.add(entity)
                    from backend.environments.session import Session
                    if isinstance(self, Session):
                        related_value._modifiers.add(self)
                # TODO: Should we remove any 'does not contains' ?
#                for v in does_not_contain:
#                    raise NotImplementedError
#        logger.debug("__mem__ set_attribute done")

    def _memory_set_value_if_not_set(self, entity, attr_name, value):
        attr = entity.entity_class.attributes[attr_name]
        try:
            self._memory_get_value(entity, attr)
        except AttributeNotLoaded:
            self._memory_set_value(entity, attr, value)

    def _memory_get_value(self, entity, attribute,
        generate_incomplete_list=False):
        """Get this environment's value for the given entity and attribute.

        Otherwise if no value exists raise AttributeNotLoaded.

        If a list is found the internal representation is returned which will
        be a local IncompleteSet or MemorySet.

        If generate_incomplete_list is True a request for a non-existant
        list-like attribute will not raise AttributeNotLoaded but will cause
        the creation of a new empty IncompleteList which is returned.

        """
        assert entity is not None
#        assert not entity._deleted # --
# deleted access of attributes is needed for post-save operations.
        assert isinstance(attribute, (Attribute))
#        logger.debug("_memory_get_value %s %s %s " % (entity, attribute, generate_incomplete_list))

        entities = None
        try:
            entities = self.values[entity.entity_class.__name__]
        except KeyError:
            raise AttributeNotLoaded("Attribute Not Loaded: (%s, %s)" % (entity, attribute.name))
        try:
            attribute_to_value = None
            try:
                attribute_to_value = entities[entity.key]
            except KeyError:
                raise AttributeNotLoaded("Attribute Not Loaded: (%s, %s)" % (entity, attribute.name))
            try:
                value = attribute_to_value[attribute.name]
                return value
            except KeyError:
                raise AttributeNotLoaded("Attribute Not Loaded: (%s, %s)" % (entity, attribute.name))
        except AttributeNotLoaded:
            if generate_incomplete_list and attribute.is_set_like:
                incomplete_list = IncompleteSet()
                self._memory_set_value(entity, attribute, incomplete_list)
                from backend.environments.session import Session
                if isinstance(self, Session):
                    incomplete_list._modifiers.add(self)
                return incomplete_list
            raise AttributeNotLoaded("Attribute Not Loaded: (%s, %s)" % (entity, attribute.name))

    # This sets a value in the value memory, it does not do anything
    # special for relationships. (see _memory_set_attribute for the handling
    # of two-way relationships)
    def _memory_set_value(self, entity, attribute, value):
        """Set a value in memory.

        This is a raw memory setting method. Entity and value must be local.

        If setting a list value no reverse values are set.
        (See _memory_set_attribute)
        """
        # -- asserts (no logic) -------------------------------
        assert entity is not None
        assert not entity._deleted
        assert attribute is not None
# Commenting out type checking.------------------
#        if attribute.value_class and not attribute.is_set_like and not attribute.related_attribute: # Inheritance poses issues here.
#            if value is not None: # We need better rules about when 'None'
#            # is acceptable
#                if not isinstance(value, attribute.value_class):
#                    raise TypeError(u'Attempted at assign %s of type %s to \
#%s.%s, which requires type %s' % (value,
#                                                                  type(value),
#                                                                  entity,
#                                                               attribute.name,
#                                                       attribute.value_class))
# End commenting out type checking.------------------
# ******** IF WE RE-ENABLE THE BELOW, DO TYPE CHECKING ***********
#        assert not isinstance(value, SetHandle) # Must call this with real
#        if isinstance(value, Entity):
#            assert value.environment == self
#        if attribute.enum and attribute.is_set_like:
#            if isinstance(value, IncompleteSet):
#                for v in value._contains:
#                    assert isinstance(v, EnumValue)
#                for v in value._does_not_contain:
#                    assert isinstance(v, EnumValue)
#            else:
#                for v in value:
#                    assert isinstance(v, EnumValue)
        # -- end asserts ---------------------------------------
#        logger.debug("__mem__     setvalue: %s.%s = %s" % (entity, attribute.name, value))
        entities = None
        try:
            entities = self.values[entity.entity_class.__name__]
        except KeyError:
            entities = {}
            self.values[entity.entity_class.__name__] = entities
        attribute_to_value = None
        try:
            attribute_to_value = entities[entity.key]
        except KeyError:
            attribute_to_value = {}
            entities[entity.key] = attribute_to_value
        attribute_to_value[attribute.name] = value

    def promote(self, entity, attribute_name, value):
        """Set the given value in memory without creating a delta."""
        self._memory_set_attribute(entity,
                                   entity.attributes[attribute_name],
                                   value)

    def patch(self, env):
        """Patch contents of the given environment into this one."""
#self.values = {} # [entity_class][key][attribute_name] = value
#self.list_handles = {} # [entity_class][attribute_name] = value
#----------------- (could this be faster?)
        # This doesn't work yet, I need a fast way to get the Entity class
        # from the entity_class_name key in value (which is lower cased.)
        notfasts = []
        for entity_class_name, key_name_dict in env.values.iteritems():
            # If this is a cache from the future, it may have values that
            # don't exist in this version, so check for them first.
            if entity_class_name not in _name_to_entity_class:
                continue
            entity_class = _name_to_entity_class[entity_class_name]
            for entity_key, value_dict in key_name_dict.iteritems():
                for attribute_name, value in value_dict.iteritems():
                    attribute = entity_class.get_attribute(attribute_name)
                    # This could be a Cache from a newer version, and for it
                    # to be backward compatible, we need to ignore attributes
                    # present in the new cache but not in the old cache.
                    if attribute and hasattr(env, '_inventory_attributes') and attribute in env._inventory_attributes:
                        if attribute.related_attribute:
                            notfasts.append((entity_class, entity_key, attribute, value))
                        else:
                            entity = self.fetch(entity_class, entity_key)
                            object.__setattr__(entity, attribute_name, value)
        for entity_class, entity_key, attribute, value in notfasts:
            peer = self.fetch(entity_class, entity_key)
            self.patch_attribute_by_name(peer, attribute.name, value)
#----------------
#        for class_name, instances in env.instances_by_class.iteritems():
#            for key, peer in instances.iteritems():
#                values = env.values[class_name][key]
#                for attribute_name, value in values.iteritems():
#                    self.patch_attribute_by_name(peer, attribute_name, value)

    def fetch(self,
              entity_class,
              key,
              has_temporary_key=False,
              write_only=False,
              *args, **kwargs):
        # key is 'value of primary key'
        assert(key is not None)

        # Get the key-to-instance dict for this class.
        class_instances = None
        try:
            class_instances = self.instances_by_class[entity_class.__name__]
        except KeyError:
            # Create and add the dict if it does not already exist.
            class_instances = {}
            self.instances_by_class[entity_class.__name__] = class_instances

        # Get the instance that goes with this key.
        obj = None
        try:
            obj = class_instances[key]
            assert(obj.environment == self)
        except KeyError:
            # Create and add the instances if does not already exist.
            # This actually creates the object without calling our special
            # __call__ method, (which is the method that calls fetch.)

            #Maybe we should be loading the obj from the environment fully here?
            obj = super(EntityMetaclass, entity_class).__call__(environment=self, key=key, has_temporary_key=has_temporary_key, write_only=write_only, *args, **kwargs)
            assert(obj.environment == self)
            class_instances[key] = obj
            # Set the key as a value in this memory.
            self._memory_set_value(obj, entity_class.get_key_attribute(), key)
        return obj

    def fetch_list(self, entity, attribute):
        # Get the key-to-instance dict for this class.
        lists = None
        try:
            lists = self.list_handles[entity.entity_class.__name__]
        except KeyError:
            # Create and add the dict if it does not already exist.
            list_handles = {}
            self.instances_by_class[entity.entity_class.__name__] = list_handles

        # Get the list that goes with this attribute
        lst = None
        try:
#            logger.debug("checking dict: '%s' for '%s'" % (cls_dict, key))
            lst = lists[attribute.name]
            assert(lst.entity == entity)
            assert(lst.entity.environment == self)
#            logger.debug("__mem__     found: %s" % lst)
        except KeyError:
            # Create and add the instances if does not already exist.
            # This actually creates the object without calling our special
            # __call__ method, (which is the method that calls fetch.)

            _list = IncompleteSet(entity, attribute)
#            logger.debug("__mem__     fetched: %s" % _list)
            lists[attribute.name] = _list
            #Maybe we should be loading the obj from the environment fully here?
            return SetHandle(entity, attribute)

        return lst

    def get(self, entity_class, key):
        no_exception = True
        try:
            instances_by_key = self.instances_by_class[entity_class.__name__]
        except KeyError:
            no_exception = False
        if no_exception:
            try:
                instance = instances_by_key[key]
            except KeyError:
                no_exception = False
        if no_exception:
            return instance
        else:
            return None
        
    def get_peer(self, entity):
        """
        Get the counterpart entity in this Environment if one exists,
        otherwise return None.

        """
        if entity.environment is self and not entity._deleted:
            return entity
        return self.get(entity.entity_class, entity.key)

    def delete(self, entity):
        """Delete entity from this environment."""
        entity = self.get_peer(entity)
        if entity is None:
            return
        if entity._deleted:
            # Redeletion is not a supported feature of Backend.
            raise NotImplementedError
        else:
            # Remove the entity from reverse relationships that hold this key.
            # (OneToMany and OneToOne data is held by the related Entity.)
            one_to_manies = []
            for attribute in entity.entity_class.attributes_order:
                if isinstance(attribute, (OneToMany, OneToOne)):
                    one_to_manies.append(attribute)
            for attribute in one_to_manies:
                # See if we have a value for the related Entity.
                try:
                    related_entity = object.__getattribute__(entity, attribute.name)
                except:
                    # If it's not on the Entity see if it is loaded in the
                    # Session.  If it's not there it will raise
                    # AttributeNotLoaded as we need to know it to delete
                    # safely.
                    # (note this assumes the Entity is a session Entity.)
                    related_entity = self.get_attribute(entity,
                                                        attribute,
                                                        create_write_only=False)
                # If the existing value is not None, add a 'set-to-None' for it.
                if related_entity:
                    from backend.environments.deltas import SetAttribute
                    self.add_delta(SetAttribute(related_entity,
                                                attribute.related_attribute,
                                                None))
# Don't do this, we need to read keys when we serialize.
#            # Set local reverse values.  (this is to ensure there are no
#            # lingering references to the deleted object.)
#            many_to_ones = []
#            for attribute in entity.entity_class.attributes_order:
#                if isinstance(attribute, (ManyToOne, FakeManyToOne)):
#                    many_to_ones.append(attribute)
#            for attribute in many_to_ones:
#                object.__setattr__(entity, attribute.name, None)
# xxx TODO: detect insufficient information from which to serialize the update.
        entity._deleted = True

    def _memory_delete_entity(self, entity):
        """Remove the Entity from this _EnvironmentMemory's storage."""
        assert entity._deleted
        try:
            entities_by_key = self.instances_by_class[entity.entity_class.__name__]
#                logger.debug("__mem__ found entities by key to delete")
        except KeyError:
            entities_by_key = {}
        try:
            del entities_by_key[entity.key]
#                logger.debug("__mem__ deleted from instances by class")
        except KeyError:
            pass

    def pretty_print(self):
        """Print a debuggable human-readable representation."""

        # TODO: this is not really complete... but is complete enough
        # for me to see my environment contains no entities.
        txt = "\n________pretty print %s___________________________________\n" % self
        txt += "instances:\n"
        for k, v in self.instances_by_class.iteritems():
            txt += k
            for k1, v1 in v.iteritems():
                txt += "     %s, %s\n" % (k1, v1)
        txt += "values:\n"
        for k, v in self.values.iteritems():
            txt += "%s - %s\n" % (k, v)
        txt += "_____________________________________________________\n"
        logger.debug(txt)
        return txt
    
    def promote_like_cache(self, entity, cache):
        """
        Promote data from the given entity's environment to this environment.
        Sufficient data to meet the needs of the given cache's
        inventory is promoted.

        """
        from backend.environments.cache import Cache
        Cache._promote_inventory_recur(self, entity, set(), cache._inventory)

    def patch_attribute_by_name(self, peer, attribute_name, value):
        """Patch is a merge-in with preference for the target."""
        if __debug__:
            logger.debug("patching %s %s %s" % (peer, attribute_name, value))
        # NOTE: THIS IS SESSION ONLY!
        existing_entity = self.fetch_peer(peer)
        attribute = peer.entity_class.get_attribute(attribute_name)
        if not attribute.related_attribute:
            object.__setattr__(existing_entity, attribute_name, value)
            return
        
        #assert not existing_entity.is_write_only # Dunno about this yet.
        #if existing_entity.is_write_to and not peer.is_write_to:
        #    existing_entity
        #attribute = peer.entity_class.get_attribute(attribute_name)
        try:
            existing_value = self._memory_get_value(existing_entity, attribute)
            has_existing_value = True
        except AttributeNotLoaded:
            has_existing_value = False

        if not attribute.enum and attribute.related_attribute:
            if attribute.is_set_like:
                if has_existing_value:
                    self._memory_set_attribute(existing_entity,
                                               attribute,
                                               patch(existing_value, value))
                else:
                    self._memory_set_attribute(existing_entity,
                                               attribute,
                                               value)
            elif not has_existing_value:
                new_value = self.get_peer(value) if value else value
                self._memory_set_attribute(existing_entity, attribute, new_value)
        elif not has_existing_value:
            self._memory_set_value(existing_entity, attribute, value)

    def promote_attribute_by_name(self, peer, attribute_name, value):
        attribute = peer.entity_class.get_attribute(attribute_name)
        self.promote_attribute(peer, attribute, value)

    def promote_attribute(self, peer, attribute, value):
        """

        """
        self._promote_attribute_one_side(peer, attribute, value)
        
        if attribute.related_attribute and not attribute.enum:
            if not attribute.related_attribute.is_set_like:
                values = [value] if value else []
            for v in values:
                self._promote_attribute_one_side(v,
                                                 attribute.related_attribute,
                                                 peer)

    # This is now static so you can promote inventory into any memory, not
    # just a cache. (see _EnvironmentMemory.promote_like_cache)
    @staticmethod
    def _promote_inventory_recur(self, peer, autoloaded, inventory):
        """Promote inventory-specified values into this cache's memory.

        self -- _EnvironmentMemory to promote data into.
        peer -- Entity from other source to promote to self.
        autoloaded -- a set of local Entities that have already had their
            autoload attributes' values loaded into this memory.
        inventory -- the compiled inventory representation to promote.

        """
        assert peer.environment != self
        entity = self.fetch_peer(peer)

        # If the inventory argument is None it means we still do autoload,
        # if we haven't already done so.
        if inventory is None and entity in autoloaded:
            return entity

        # This is a dict of attributes we'll be promoting.
        local_inventory = {}

        # Only check autoloads not already included.
        if entity not in autoloaded:
            for a in entity.autoloads:
                local_inventory[a] = None

        # If there is anything in inventory, add it to local_inventory.
        if inventory is not None:
            local_inventory.update(inventory)

        # Mark this entity as having been autoloaded.
        autoloaded.add(entity)

        # For each item in the local inventory, promote.
        for attribute, sub_cache in local_inventory.iteritems():
            # This assertion, sadly won't work entirely because of
            # subtlies surrounding inheritance.
#            assert attribute in self._inventory_attributes, '%s not in %s._inventory_attributes' % (attribute, self)

            # If we already have the attribute, we don't need to do anything.
            # We are going to check if we already have the attribute.
            already_has_attribute = False

            # If we raise an AttributeNotLoaded here, then we don't have it.
            try:
                # See what we keep in local memory.
                val = self._memory_get_value(entity,
                                             attribute,
                                             generate_incomplete_list=False)
                # If the value is an Entity, we may need to adjust or promote.
                if isinstance(val, Entity):
                    # If the value is a write_only, we need to replace that
                    # with a proper value copied from the peer.
                    if val.write_only:
                        # In this case we need to get the peer from the other
                        # source, and update the write_only.
                        peer_val = getattr(peer, attribute.name)
                        val = self.fetch_peer(peer_val)
                        # If we've already written some data on, this will
                        # adjust that data over.
                        self._adjust_if_write_only_connector(entity,
                                                             attribute,
                                                             val)
                    else:
                        # If it isn't a write_only, just call fetch.
                        peer_val = peer.environment.fetch_peer(val)

                    # We may need to autoload its values too.
                    if val not in autoloaded or sub_cache:
                        peer_val = getattr(peer, attribute.name)
                        # What if val is none in the environment?
                        self._promote_inventory_recur(
                            self,
                            peer_val,
                            autoloaded,
                            sub_cache)
                if not isinstance(val, IncompleteSet):
                    already_has_attribute = True
            except AttributeNotLoaded:
                pass
            if not already_has_attribute:
                # Get the value this entity has.
                val = getattr(peer, attribute.name)
                if attribute.enum:
                    if attribute.is_set_like:
                        # Special case conversion. (Where else do we do this?
                        # can it consolodate? This is essentially "promote from
                        # persistent".)
                        if isinstance(peer, Persistent):
                            val = [attribute.enum[v.number] for v in val]

                        if __debug__: # ??? remove the below?
                            for v in val:
                                assert isinstance(v, EnumValue)
                # Memory's set_attribute converts all values to what it needs.
                # We don't want this... it clears reverse values, it sets up
                # reverse values. We handle all of that on our own in a cache.
                # So just use _set_value, but do your own conversion.
                new_val = val
                if isinstance(val, SetHandle):
                    new_val = val._make_local_copy_of_value(self)
#                    logger.debug("__ec__ converted SetHandle to: %s" % val)
                elif isinstance(val, (Entity, Persistent)):
                    new_val = self.fetch_peer(val)
                elif new_val is not None and attribute.related_attribute and not attribute.related_attribute.entity_class.persistent_class.saves_to_postgresql:
                    new_val = self.fetch(attribute.related_attribute.entity_class, new_val)
                elif attribute.is_set_like:
                    assert isinstance(val, (InstrumentedList, list))
                    # If it isnt a list handle it can still be a list, so localize it.
                    l = MemorySet()
                    for v in val:
                        if attribute.enum:
                            l.add(v)
                        else:
                            l.add(self.fetch_peer(v))
                    new_val = l

                # When we do a migration that adds a counter field the value
                # begins as None.  We need to read this as the default.
#                if new_val is None and type(attribute).__name__ == 'Incrementer':
#                    new_val = attribute.default

#                if attribute.enum
#                    assert new_val is not None
                # Do we have a presense in the reverse value of this object?
#                logger.debug("__ec__ checking for special reverse value.")
                if new_val is not None and not attribute.enum:
#                    logger.debug("__ec__ new_val is not None (and not enum)")
                    if attribute.related_attribute:
#                        logger.debug("__ec__ has related attribute")
                        if attribute.is_set_like:
#                            logger.debug("__ec__ is list like : %s" % new_val)
                            if isinstance(new_val, IncompleteSet):
#                                logger.debug("__ec__ is IncompleteSet")
                                for v in new_val._contains:
                                    if attribute.related_attribute.is_set_like:
                                        self._memory_get_value(v,
                                                               attribute.related_attribute,
                                                               generate_incomplete_list=True).add(entity)
                                    else:
                                        self._memory_set_value(v, attribute.related_attribute, entity)
#                                logger.debug("__ec__ done with incomplete set.")
                            else:
#                                logger.debug("__ec__ is not IncompleteSet")
                                for v in new_val:
                                    if attribute.related_attribute.is_set_like:
#                                        logger.debug("__ec__ list style")
#                                        logger.debug("%s -- %s" % (attribute, attribute.related_attribute))
                                        self._memory_get_value(v,
                                                               attribute.related_attribute,
                                                               generate_incomplete_list=True).add(entity)
#                                        logger.debug("__ec__ what's this? %s" % self._memory_get_value(v, attribute.related_attribute))
                                    else:
#                                        logger.debug("__ec__ single style")
                                        self._memory_set_value(v, attribute.related_attribute, entity)
#                                logger.debug("__ec__ is not IncompleteSet Done")
                        else:
                            if attribute.related_attribute.is_set_like:
                                self._memory_get_value(new_val,
                                                       attribute.related_attribute,
                                                       generate_incomplete_list=True).add(entity)
                            else:
#                                logger.debug("%s -- %s" % (attribute, attribute.related_attribute))
                                self._memory_set_value(new_val,
                                                       attribute.related_attribute,
                                                       entity)

#                logger.debug("__ec__ done checking.")
                # kludge.
                if attribute.value_class == str and new_val is not None:
                    # MainPersistent (maybe?) is giving me unicode.
                    new_val = str(new_val)
                # XXX REMOVE THIS HACK AFTER THE ENUM MIGRATE FIX IS COMPLETE -- MEC
                # xxx and incr fix.
                if new_val is None:
                    if attribute.enum: # xxx remove me
                        new_val = attribute.enum[0]
                    elif type(attribute).__name__ == 'Incrementer':
                        new_val = 0
                    #logger.error('xxx promote calling memory set val %s %s %s' % (entity, attribute.name, new_val))
                self._memory_set_value(entity, attribute, new_val)

                # TODO: we need to be mindful of reverse values... in the
                # tests... duh.... must not be autoload... yeah?
                # TODO: If val is a list, we need to auto-call setup_peer
                # with each val for its entity classes's autoloads.
                # Question: do we do that here or in set_attribute?
                # Let's do it here for now, and if that doesn't work move it in.
                # (why wouldn't it go in set_attribute? because the presence of
                # those values is only relevant to a cache. (hmm?))
#                logger.debug("__ec__ val: %s" % val)

                # Make sure reverse values are set up, and that we recurse
                # if needed.
                # Note: 'val' (must be?) is from the source, new_val is local.
                if attribute.related_attribute and not attribute.enum:
                    # Non SQL entities just need the reverse relationship
                    # we set above, so this empty list is a 'skip'.
                    if not attribute.related_attribute.entity_class.persistent_class.saves_to_postgresql:
                        vals = []
                    elif attribute.is_set_like: # TODO what if it's an IncompleteSet?
#                        logger.debug("__ec__ is list")
                        if isinstance(val, SetHandle):
                            val = val._get_value()
                            if isinstance(val, IncompleteSet):
                                val = val._contains
                        vals = val
                    else:
                        if val is None:
                            vals = []
                        else:
                            vals = [val]
                    for v in vals:
                        if v not in autoloaded or sub_cache:
                            _EnvironmentMemory._promote_inventory_recur(self,
                                                          v,
                                                          autoloaded,
                                                          sub_cache)
        return entity

    
    def promote_environment(self, env):
        """Promote contents of 'env' to this Cache."""
        #logger.debug("%s promoting %s" % (self, env))
        #logger.debug(env.pretty_print())
        for class_name, instances in env.instances_by_class.iteritems():
            #logger.debug("instances %s" % instances)
            #logger.debug("self %s" % self.pretty_print())
            for key, peer in instances.iteritems():
                try:
                    values = env.values[class_name][key]
                    #logger.debug("values: %s" % values)
                except KeyError:
                    #logger.debug("values none")
                    values = None
                if values:
                    for attribute_name, value in values.iteritems():
                        self.promote_attribute_by_name(peer, attribute_name, value)

# A handle to a list in an environment. Allows you to change anything about
# the list (including replacing its value entirely) and yet allow previous
# fetches to still be valid.
class SetHandle(object):

    def __init__(self, entity, attribute):
        assert isinstance(entity, Entity)
        self.entity = entity
        assert isinstance(attribute, (Attribute))
        self.attribute = attribute

    def _get_value(self):
        return self.entity.environment._memory_get_value(self.entity,
                                                         self.attribute)

    def _do_add(self, value):
        if isinstance(value, (Persistent, Entity)):
            value = self.entity.environment.fetch_peer(value)
        #logger.debug("_do_add %s" % value)
        logger.debug(self._get_value())
        return self._get_value().add(value)

    @deprecated
    def append(self, value):
        logger.warn("SetHandle -- Append is deprecated, use 'add'.")
        return self.add(value)

    def add(self, value):
#        logger.debug("SetHandle.add(%s)" % value)
        if isinstance(value, (Persistent, Entity)):
            value = self.entity.environment.fetch_peer(value)
        from backend.environments.session import Session
        if isinstance(self.entity.environment, Session):
            append_type = Session.get_append_delta_for_attribute(
                self.attribute)
            delta = append_type(self.entity,
                                self.attribute,
                                value,
                                self.attribute.related_attribute)
            self.entity.environment.add_delta(delta)
            self._get_value()._modifiers.add(self.entity.environment)
            # was altered by session-append
        self._do_add(value)

        # Do the reverse.
        if not self.attribute.enum and self.attribute.related_attribute:
            #logger.debug("__sh__ do reverse")
            related_attribute = self.attribute.related_attribute
            assert isinstance(value, Entity)
            assert value.environment == self.entity.environment
            if related_attribute.is_set_like:
                related_value = getattr(value, related_attribute.name)
                # What if value does not exist in... value's environment???
                assert isinstance(related_value, SetHandle)
                related_value._do_add(self.entity)
                if isinstance(self.entity.environment, Session):
                    related_value._get_value()._modifiers.add(self.entity.environment)
                    # set was altered by session-append
            else:
                value.environment._memory_set_value(value,
                                                    related_attribute,
                                                    self.entity)

    def _do_discard(self, value):
        if isinstance(value, (Persistent, Entity)):
            value = self.entity.environment.fetch_peer(value)
        return self._get_value().discard(value)

    def discard(self, value):
        if isinstance(value, (Persistent, Entity)):
            value = self.entity.environment.fetch_peer(value)
        # Make a break-relationship delta and add it to the session.
        # Make a Make-relationship delta and add it to the session.
#        logger.debug("__sh__ REMOVING FROM MEMORY LIST: %s" % value)
        # TODO: This is a hack.... we need a session's SetHandle
        # to be equipped in a way a memory's is not.
        # It's a leeeetle too fancy for the moment, so here's some grody
        # code.
        from backend.environments.session import Session
        if isinstance(self.entity.environment, Session):
            delta_type = Session.get_discard_delta_for_attribute(self.attribute)
            if not self.attribute.enum:
                value = self.entity.environment.get_peer(value)
            delta = delta_type(self.entity, self.attribute, value, self.attribute.related_attribute)
            self.entity.environment.add_delta(delta)
            self._get_value()._modifiers.add(self.entity.environment)

        self._do_discard(value)

        # Do the reverse.
        if not self.attribute.enum:
            related_attribute = self.attribute.related_attribute
            assert isinstance(value, Entity)
            assert value.environment == self.entity.environment
            if related_attribute:
                if related_attribute.is_set_like:
    #                logger.debug('__sh__ getting related value')
                    related_value = getattr(value, related_attribute.name)
    #                logger.debug("__sh__ related value: %s" % related_value)
    #                assert isinstance(value, SetHandle)
                    related_value._do_discard(self.entity)
                    if isinstance(self.entity.environment, Session):
                        related_value._get_value()._modifiers.add(self)
                else:
                    value.environment._memory_set_value(value, related_attribute, None)

    def __contains__(self, value):
#        logger.debug("__sh__ contains?: %s : %s" % (value, value.__hash__()))
        #v = self._get_value()
        return value in self._get_value()

    def __nonzero__(self):
        return bool(self._get_value())

    def __eq__(self, other):
        #TODO: Add support for lists, InstrumentList, frozenset
        if isinstance(other, (set, MemorySet)):
            return self._get_value() == other
        if isinstance(other, IncompleteSet):
            #IncompleteSet currently doesn't have a valid __eq__ so this is
            # needed to keep return values consistent
            raise NotImplementedError
        elif isinstance(other, SetHandle):
            return self._get_value() == other._get_value()
        else:
            return False

    def __len__(self):
        return len(self._get_value())

    def __iter__(self):
        return self._get_value().__iter__()

    def __getitem__(self, key):
        return self._get_value().__getitem__(key)

    def __and__(self, other):
        return self._get_value().__and__(other)

    def __rand__(self, other):
        return self._get_value().__and__(other)

    def __or__(self, other):
        return self._get_value().__or__(other)

    def __ror__(self, other):
        return self._get_value().__or__(other)

    def __sub__(self, other):
        return self._get_value().__sub__(other)

    def __rsub__(self, other):
        return other.__sub__(self._get_value())

    def get_one(self):
        return self._get_value().get_one()

    def _make_local_copy_of_value(self, environment):
        value = self._get_value()
#        logger.debug("__sh__ making local copy, this is internal: %s" % value)
        if isinstance(value, IncompleteSet):
            new_contains = []
            new_does_not_contain = []
            for v in value._contains:
                if self.attribute.enum:
                    new_contains.append(v)
                else:
                    new_contains.append(environment.fetch_peer(v))
            for v in value._does_not_contain:
                if self.attribute.enum:
                    new_does_not_contain.append(v)
                else:
                    new_does_not_contain.append(environment.fetch_peer(v))
            l = IncompleteSet()
            l._contains = set(new_contains)
            l._does_not_contain = set(new_does_not_contain)
            value = l
        elif isinstance(value, MemorySet):
            l = MemorySet()
            for v in value:
                if self.attribute.enum:
                    l.add(v)
                else:
                    l.add(environment.fetch_peer(v))
            value = l
        else:
            raise NotImplementedError
        assert isinstance(value, (IncompleteSet, MemorySet))
#        logger.debug("__sh__ _make_local_copy_of_value returning: %s" % value)
        return value

    def __repr__(self):
        try:
            return "%s(%s, %s)->%s" % (self.__class__.__name__, self.entity, self.attribute.name, self._get_value())
        except AttributeNotLoaded:
            return "%s(%s, %s)->(not loaded)" % (self.__class__.__name__, self.entity, self.attribute.name)
