import collections.abc
from typing import Union

import bson
import h5py
from ._vendor import mongomock
from ._vendor.mongomock import store as mongomock_store
from ._vendor.mongomock import thread as mongomock_thread
import numpy as np

from . import stores

__all__ = "ServerStore", "DatabaseStore", "CollectionStore"


class ServerStore(stores.ServerStore):
    def __init__(self, filename: str, mode="a"):
        self._databases = h5py.File(filename, mode)

    def __getitem__(self, db_name) -> "DatabaseStore":
        db: h5py.Group = self._databases.require_group(db_name)
        return DatabaseStore(db)

    def __contains__(self, db_name):
        return self[db_name].is_created

    def list_created_database_names(self):
        return [name for name in self._databases if self[name].is_created]

    def close(self):
        self._databases.close()


class DatabaseStore(mongomock_store.DatabaseStore):
    """Object holding the data for a database (many collections)."""

    def __init__(self, group: h5py.Group):
        self._group = group
        self._collections = {}

    def __getitem__(self, col_name) -> "CollectionStore":
        try:
            return self._collections[col_name]
        except KeyError:
            coll = CollectionStore(self._group.require_group(col_name))
            self._collections[col_name] = coll
            return coll

    def __contains__(self, col_name) -> bool:
        if col_name not in self._collections:
            return False

        return self[col_name].is_created

    def list_created_collection_names(self):
        return [name for name in self._collections if self[name].is_created]

    def create_collection(self, name):
        col = self[name]
        col.create()
        return col

    def rename(self, name, new_name):
        existing = self._group.get(new_name)
        if existing is not None:
            del self._group[new_name]

        self._group.move(name, new_name)
        self._collections[new_name] = self._collections.pop(name)

    @property
    def is_created(self):
        return any(self[coll_name].is_created for coll_name in self._collections)


class CollectionStore(mongomock_store.CollectionStore):
    """Object holding the data for a collection."""

    DOCUMENTS = "documents"
    INDEXES = "indexes"
    IS_FORCE_CREATED = "is_force_created"
    TTL_INDEXES = "ttl_indexes"

    EMPTY_UTF16 = "".encode("utf16")

    def __init__(self, group: h5py.Group):
        self._group = group

        try:
            group.create_dataset(self.IS_FORCE_CREATED, data=False)
        except ValueError:
            pass

        try:
            doc_group = group[self.DOCUMENTS]
        except KeyError:
            doc_group = group.create_group(self.DOCUMENTS, track_order=True)
        self._documents = GroupDict(doc_group)

        self.indexes = IndexDict(group.require_group(self.INDEXES))
        self._ttl_indexes = GroupDict(group.require_group(self.TTL_INDEXES))

        self._rwlock = mongomock_thread.RWLock()

    @property
    def _is_force_created(self) -> bool:
        return self._group[self.IS_FORCE_CREATED][()]

    @_is_force_created.setter
    def _is_force_created(self, new_val: bool):
        self._group[self.IS_FORCE_CREATED][()] = new_val

    def create(self):
        self._is_force_created = True

    @property
    def is_created(self):
        return self._documents or self.indexes or self._is_force_created

    def drop(self):
        self._documents.clear()
        self.indexes.clear()
        self._ttl_indexes.clear()
        self._is_force_created = False

    def create_index(self, index_name: str, index_dict: dict):
        self.indexes[index_name] = index_dict
        if index_dict.get("expireAfterSeconds") is not None:
            self._ttl_indexes[index_name] = index_dict

    def drop_index(self, index_name: str):
        self._remove_expired_documents()

        # The main index object should raise a KeyError, but the
        # TTL indexes have no meaning to the outside.
        del self.indexes[index_name]
        self._ttl_indexes.pop(index_name, None)

    def __contains__(self, key):
        return super().__contains__(self._encode_key(key))

    def __getitem__(self, key: str):
        return super().__getitem__(self._encode_key(key))

    def __setitem__(self, key, val: dict):
        super().__setitem__(self._encode_key(key), val)

    def __delitem__(self, key):
        super().__delitem__(self._encode_key(key))

    def _encode_key(self, key) -> Union[str, bytes]:
        if key == "":
            return self.EMPTY_UTF16

        return str(key)


class GroupDict(collections.abc.MutableMapping):
    """Object that stores MongoDB documents in a HDF5 group

    Storage of MongoDB documents is done as a BSON.encoded uint8 variable length array, using the trick discussed here:
    https://github.com/mila-iqia/fuel/issues/360#issuecomment-237890510
    """

    def __init__(self, group: h5py.Group):
        self._group = group

    @property
    def group(self) -> h5py.Group:
        return self._group

    def __getitem__(self, key: str) -> dict:
        return self._decode_doc(self._group[key])

    def __iter__(self):
        return self._group.__iter__()

    def __len__(self):
        return self._group.__len__()

    def __setitem__(self, key: str, value: dict):
        # Doing this emulates normal python dictionary behaviour
        # i.e. assigning a value to an existing key just overwrites it
        value = self._encode_doc(value)
        try:
            ds = self._group[key]
        except KeyError:
            dt = h5py.vlen_dtype(np.dtype("uint8"))
            ds = self._group.create_dataset(key, shape=(), dtype=dt)

        ds[()] = value

    def __delitem__(self, key: str):
        del self._group[key]

    def _encode_doc(self, doc: dict):
        encoded = bson.encode(doc)
        return np.frombuffer(encoded, dtype="uint8")

    def _decode_doc(self, dataset) -> dict:
        encoded = dataset[()].tobytes()
        return bson.decode(encoded)


class IndexDict(GroupDict):
    """Specialised group dictionary that stores MongoDB index information.

    This is necessary because index key specifications are given as tuples while BSON which is used to store
    the index dictionary converts these to lists.  Here we intercept decoding and convert back to tuples.
    """

    def _decode_doc(self, dataset) -> dict:
        index_dict = super()._decode_doc(dataset)
        # Convert list of index keys to tuples as they should be
        index_dict["key"] = list(map(tuple, index_dict["key"]))
        return index_dict
