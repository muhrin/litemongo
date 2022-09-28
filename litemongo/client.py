from urllib import parse
from typing import Union

from ._vendor import mongomock

from . import stores

__all__ = "MongoClient", "StoreType", "connect"


class StoreType:
    H5PY = "h5py"
    PYTABLES = "pytables"
    SQLITE = "sqlite"
    MEMORY = "memory"


def create_store(spec: Union[stores.ServerStore, str]) -> stores.ServerStore:
    """Server store factory"""
    if isinstance(spec, stores.ServerStore):
        return spec

    if isinstance(spec, str):
        uri = parse.urlparse(spec)
        store_type = StoreType.SQLITE  # The default

        if uri.query:
            qs_parsed = parse.parse_qs(uri.query)
            store_type = qs_parsed.pop("engine", store_type)

        if store_type == StoreType.H5PY:
            from . import h5py_store

            return h5py_store.ServerStore(uri.path)
        if store_type == StoreType.PYTABLES:
            from . import pytables_store

            return pytables_store.ServerStore(uri.path)
        if store_type == StoreType.SQLITE:
            from . import sqlite_store

            return sqlite_store.ServerStore(uri.path)
        if store_type == StoreType.MEMORY:
            from ._vendor.mongomock import store as mongomock_store

            return mongomock_store.ServerStore()

    raise TypeError(f"Unexpected server store specification: {spec}")


class MongoClient(mongomock.MongoClient):
    def __init__(
        self,
        store: stores.ServerStore,
        database: str = "",
        document_class=dict,
        tz_aware=False,
        read_preference=None,
    ):
        # Create a URI string with the (optional) default database
        uri = f"mongodb://localhost/{database}"
        super().__init__(
            host=uri,
            document_class=document_class,
            tz_aware=tz_aware,
            read_preference=read_preference,
            _store=create_store(store),
        )

    def __enter__(self) -> "MongoClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        super().close()
        # Make sure to close the store
        self._store.close()


def connect(
    path: str,
    document_class=dict,
    tz_aware=False,
    read_preference=None,
) -> MongoClient:
    """Convenience function to build a MongoClient.  This will create a server store based on the
    passed path string"""
    parsed = parse.urlparse(path)
    database = parsed.fragment
    return MongoClient(
        create_store(path),
        database=database,
        document_class=document_class,
        tz_aware=tz_aware,
        read_preference=read_preference,
    )
