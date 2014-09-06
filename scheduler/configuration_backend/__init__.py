import logging
log = logging.getLogger('scheduler.configuration_backend')

import collections


class ABCTasksConfigBase(object):
    def __getitem__(self, key):
        """
        This should return the appropriate TasksConfig instance
        if the gotten value is a mapping or sequence.
        """
        # For example:
        # if isinstance(val, (list, tuple)):
        #     return MySubclassOfTasksConfigBaseSequence(val)
        # elif isinstance(val, dict):
        #     return MySubclassOfTasksConfigBaseMapping(val)
        raise NotImplementedError("You need to write this")

    def __len__(self):
        raise NotImplementedError("You need to write this")


class TasksConfigBaseMapping(ABCTasksConfigBase, collections.Mapping):
    """Abstract Base Class interface for all configuration backends.
    This implements a read-only dictionary

    Any TasksConfig object that is a key:value mapping should
    inherit from this class"""

    def __iter__(self):
        raise NotImplementedError("You need to write this")

    def __repr__(self):
        return "TasksConfigMapping<%s keys:%s>" % (
            self.__class__.__name__, len(self))

    def __eq__(self, other):
        if isinstance(other, TasksConfigBaseMapping):
            return list(other) == list(self)
        else:
            return False


class TasksConfigBaseSequence(ABCTasksConfigBase, collections.Sequence):
    """Abstract Base Class interface for all configuration backends.
    This implements an immutable sequence.

    Any TasksConfig object that is a sequence should inherit from this class
    """

    def __repr__(self):
        return "TasksConfigSequence<%s keys:%s>" % (
            self.__class__.__name__, len(self))

    def __eq__(self, other):
        if isinstance(other, TasksConfigBaseSequence):
            return list(other) == list(self)
        else:
            return False

    def __ne__(self, other):
        return not self == other
