import abc

from ._vendor.mongomock import store as mongomock_store


__all__ = ("ServerStore",)


class ServerStore(mongomock_store.ServerStore, metaclass=abc.ABCMeta):
    """MongoMock ServerStore with some additional functionality"""

    @abc.abstractmethod
    def close(self):
        """Close the store"""
