"""Definition of MongoClient and methods to create one"""

import pathlib
from typing import Dict, Union
from urllib import parse

from . import stores
from ._vendor import mongomock
from ._vendor.mongomock import store as mongomock_store

__all__ = "MongoClient", "StoreType", "connect"

# In-memory stores keyed by URI location, so connecting to the same URI twice sees the same data
_MEMORY_STORES: Dict[str, mongomock_store.ServerStore] = {}


class StoreType:
    H5PY = "h5py"
    PYTABLES = "pytables"
    SQLITE = "sqlite"
    MEMORY = "memory"


def create_store(
    spec: Union[mongomock_store.ServerStore, str, parse.ParseResult],
) -> mongomock_store.ServerStore:
    """Server store factory"""
    # pylint: disable=import-outside-toplevel
    # Check against the mongomock base class so the in-memory store is accepted too
    if isinstance(spec, mongomock_store.ServerStore):
        return spec

    if isinstance(spec, (str, parse.ParseResult)):
        if isinstance(spec, str):
            uri = parse.urlparse(spec)
        else:
            uri = spec

        store_type = StoreType.SQLITE  # The default

        if uri.query:
            qs_parsed = parse.parse_qs(uri.query)
            store_type = qs_parsed.pop("engine", store_type)
            if len(store_type) != 1:
                raise ValueError(f"engine must contain a single entry, got {store_type}")
            store_type = store_type[0]

        if store_type == StoreType.H5PY:
            from . import h5py_store

            return h5py_store.ServerStore(_parse_path(uri))
        if store_type == StoreType.PYTABLES:
            from . import pytables_store

            return pytables_store.ServerStore(_parse_path(uri))
        if store_type == StoreType.SQLITE:
            from . import sqlite_store

            return sqlite_store.ServerStore(_parse_path(uri))
        if store_type == StoreType.MEMORY:
            key = uri.netloc + uri.path
            if key not in _MEMORY_STORES:
                _MEMORY_STORES[key] = mongomock_store.ServerStore()
            return _MEMORY_STORES[key]

    raise TypeError(f"Unexpected server store specification: {spec}")


class MongoClient(mongomock.MongoClient):
    # pylint: disable=abstract-method
    def __init__(
        self,
        store: stores.ServerStore,
        database: str = "",
        *,
        document_class=dict,
        tz_aware=False,
        read_preference=None,
        **kwargs,
    ):
        # Create a URI string with the (optional) default database
        uri = f"mongodb://localhost/{database}"
        super().__init__(
            host=uri,
            document_class=document_class,
            tz_aware=tz_aware,
            read_preference=read_preference,
            _store=create_store(store),
            **kwargs,
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
    path: Union[str, parse.ParseResult],
    document_class=dict,
    tz_aware=False,
    read_preference=None,
    **kwargs,
) -> MongoClient:
    """Convenience function to build a MongoClient.  This will create a server store based on the
    passed path string"""
    if isinstance(path, str):
        parsed = parse.urlparse(path)
    else:
        parsed = path
    database = parsed.fragment
    return MongoClient(
        create_store(path),
        database=database,
        document_class=document_class,
        tz_aware=tz_aware,
        read_preference=read_preference,
        **kwargs,
    )


def _parse_path(uri: parse.ParseResult) -> pathlib.Path:
    return pathlib.Path(uri.netloc) / pathlib.Path(uri.path)
