import logging
log = logging.getLogger('scheduler.configuration_backend')

import collections


class TasksConfigBase(collections.Mapping):
    """Abstract Base Class interface all configuration backends.
    This implements a read-only dictionary"""

    def __getitem__(self, key):
        # fetch the configuration for an app_name.  This can return a dict,
        # but could also return a new instance of this class or some kind
        # of collections.Mapping  - Remember, a collections.Mapping object is
        # immutable while a normal dict is mutable.
        raise NotImplementedError("You need to write this")

    def __iter__(self):
        # iterate over all known app_names
        raise NotImplementedError("You need to write this")

    def __len__(self):
        # number of apps recognized by the scheduler
        raise NotImplementedError("You need to write this")

    def __repr__(self):
        return "TasksConfig<%s keys:%s>" % (
            self.__class__.__name__, len(self))
