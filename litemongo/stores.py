import abc

import mongomock.store

__all__ = ('ServerStore',)


class ServerStore(mongomock.store.ServerStore, metaclass=abc.ABCMeta):
    """MongoMock ServerStore with some additional functionality"""

    @abc.abstractmethod
    def close(self):
        """Close the store"""
