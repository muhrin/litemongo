import collections.abc
import pathlib
import sqlite3
from typing import Iterator, Union

import bson
import bson.json_util
import mongomock
import mongomock.store
import mongomock.thread

from . import stores

__all__ = 'ServerStore', 'DatabaseStore', 'CollectionStore'


class ServerStore(stores.ServerStore):
    def __init__(self, dirpath: str):
        self._dirpath = pathlib.Path(dirpath).absolute()
        self._databases = {}

        if self._dirpath.exists():
            if not self._dirpath.is_dir():
                raise ValueError(f'Path exists but is not a directory: {self._dirpath}')
        else:
            self._dirpath.mkdir(parents=True)

    def __getitem__(self, db_name) -> 'DatabaseStore':
        if db_name not in self._databases:
            self._databases[db_name] = DatabaseStore(self._dirpath / db_name)

        return self._databases[db_name]

    def __contains__(self, db_name):
        return self[db_name].is_created

    def list_created_database_names(self):
        return [name for name in self._databases if self[name].is_created]

    def close(self):
        pass


class DatabaseStore(mongomock.store.DatabaseStore):
    """Object holding the data for a database (many collections)."""

    def __init__(self, path: pathlib.Path):
        self._dirpath = path.absolute()
        self._collections = {}

        if self._dirpath.exists():
            if not self._dirpath.is_dir():
                raise ValueError(f'Path exists but is not a directory: {self._dirpath}')
        else:
            self._dirpath.mkdir(parents=True)

    def __getitem__(self, col_name) -> 'CollectionStore':
        if col_name not in self._collections:
            self._collections[col_name] = CollectionStore(self._dirpath / col_name)

        return self._collections[col_name]

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

    def rename(self, name: str, new_name: str):
        coll = self._collections.pop(name)
        coll.rename(new_name)
        self._collections[new_name] = coll

    @property
    def is_created(self):
        return any(self[coll_name].is_created for coll_name in self._collections)


class CollectionStore(mongomock.store.CollectionStore):
    """Object holding the data for a collection."""

    DOCUMENTS = 'documents'
    INDEXES = 'indexes'

    ID = '_id'
    DOC = 'doc'

    _documents = None
    indexes = None

    def __init__(self, filename: pathlib.Path):
        self._path = filename.absolute()
        self._is_force_created = False
        self._ttl_indexes = {}

        self.open()
        self._rwlock = mongomock.thread.RWLock()

    def create(self):
        self._is_force_created = True
        self._path.touch(exist_ok=True)

    @property
    def is_created(self):
        return not self.is_empty or self.indexes or self._is_force_created

    @property
    def documents(self):
        yield from self._documents.values()

    def __len__(self) -> int:
        return len(self._documents)

    def rename(self, new_name: str):
        self.close()
        self._path = self._path.rename(self._path.parent / new_name)
        self.open()

    def drop(self):
        self.close()
        self._path.unlink(missing_ok=True)

        self._ttl_indexes.clear()
        self._is_force_created = False

        self.open()

    def create_index(self, index_name: str, index_dict: dict):
        self.indexes[index_name] = index_dict
        if index_dict.get('expireAfterSeconds') is not None:
            self._ttl_indexes[index_name] = index_dict

    def drop_index(self, index_name: str):
        self._remove_expired_documents()

        # The main index object should raise a KeyError, but the
        # TTL indexes have no meaning to the outside.
        del self.indexes[index_name]
        self._ttl_indexes.pop(index_name, None)

    def open(self):
        self._connection = sqlite3.connect(self._path)
        self._cur = self._connection.cursor()
        self._documents = TableDict(self.DOCUMENTS, self._connection, self._cur)
        self.indexes = IndexDict(self.INDEXES, self._connection, self._cur)

    def close(self):
        self._cur.close()
        self._cur = None
        self._connection.close()
        self._connection = None
        self._documents = None
        self.indexes = None


class TableDict(collections.abc.MutableMapping):
    ID = '_id'
    DOC = 'doc'

    def __init__(
            self,
            table_name: str,
            connection: sqlite3.Connection,
            cursor: sqlite3.Cursor
    ):
        self._table_name = table_name
        self._connection = connection
        self._cur = cursor
        self._init_table()

    def _init_table(self):
        try:
            self._cur.execute(
                f"CREATE TABLE IF NOT EXISTS {self._table_name}("
                f"{self.ID} PRIMARY KEY,"
                f"{self.DOC}"
                f")"
            )
        except sqlite3.OperationalError as exc:
            print(exc)
        else:
            self._connection.commit()

    def __len__(self) -> int:
        res = self._cur.execute(f"select count(*) from {self._table_name}")
        counts = res.fetchone()
        return counts[0]

    def __iter__(self):
        cur = self._connection.cursor()
        try:
            for row in cur.execute(f"SELECT {self.ID} from {self._table_name} ORDER BY ROWID"):
                yield row[0]
        finally:
            cur.close()

    def __contains__(self, key) -> bool:
        key = self._encode_key(key)
        res = self._cur.execute(f"SELECT {self.ID} from {self._table_name} where {self.ID}='{key}'")
        return res.fetchone() is not None

    def __getitem__(self, key: str):
        key = self._encode_key(key)
        res = self._cur.execute(f"SELECT {self.DOC} from {self._table_name} where {self.ID}='{key}'")
        value = res.fetchone()
        if value is None:
            raise KeyError(key)

        return self._decode_doc(value[0])

    def __setitem__(self, key, val: dict):
        key = self._encode_key(key)
        val = self._encode_doc(val)
        data = (key, val, val)
        with self._connection:
            self._cur.execute(
                f"INSERT INTO {self._table_name} VALUES(?, ?)"
                f"ON CONFLICT({self.ID}) DO UPDATE SET {self.DOC}=?", data
            )

    def __delitem__(self, key):
        key = self._encode_key(key)
        res = self._cur.execute(f"DELETE FROM {self._table_name} WHERE {self.ID}='{key}'")
        if res.rowcount <= 0:
            raise KeyError(key)

    def values(self) -> Iterator[dict]:
        cur = self._connection.cursor()
        try:
            for row in cur.execute(f"SELECT {self.DOC} from {self._table_name} ORDER BY ROWID"):
                yield self._decode_doc(row[0])
        finally:
            cur.close()

    def _encode_key(self, key) -> str:
        return str(key)

    def _encode_doc(self, doc: dict) -> Union[str, bytes]:
        """Encode the document dictionary for storing in the backend

        We have also tried bson.json_util.dumps(doc) but this adds a significant computational cost.

        :param doc: the document to be encoded
        :return: the encoded version that can be decoded back using _decode_doc()
        """
        return bson.encode(doc)

    def _decode_doc(self, encoded: Union[str, bytes]) -> dict:
        """
        Decode a document dictionary previously encoded using _encode_doc()

        :param encoded: the encoded document
        :return: the decoded document dictionary
        """
        return bson.decode(encoded)


class IndexDict(TableDict):
    """Specialised group dictionary that stores MongoDB index information.

    This is necessary because index key specifications are given as tuples while BSON which is used to store
    the index dictionary converts these to lists.  Here we intercept decoding and convert back to tuples.
    """

    def _decode_doc(self, dataset) -> dict:
        index_dict = super()._decode_doc(dataset)
        # Convert list of index keys to tuples as they should be
        index_dict['key'] = list(map(tuple, index_dict['key']))
        return index_dict