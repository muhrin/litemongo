"""SQLite backed store.

Each database is a directory and each collection is an SQLite file within it.  Documents are kept
as BSON blobs keyed by their (encoded) id.  Queries are evaluated by mongomock, with the help of
in-process hash indexes (see ``_vendor/mongomock/indexing.py``) that are rebuilt whenever another
connection changes the file.
"""

import collections.abc
import datetime
import pathlib
import sqlite3
from typing import Iterable, Iterator, Union
import uuid

import bson

from . import stores
from ._vendor.mongomock import helpers
from ._vendor.mongomock import store as mongomock_store
from ._vendor.mongomock import thread as mongomock_thread

__all__ = "ServerStore", "DatabaseStore", "CollectionStore"

# Files that SQLite keeps next to a database file, these are not collections
_SQLITE_SIDECAR_SUFFIXES = ("-wal", "-shm", "-journal")

# Maximum number of parameters to use in a single 'IN (...)' query
_MAX_PARAMS = 500

# How document ids are turned into SQLite keys.  Files created before the format was recorded use
# the legacy format, where e.g. ObjectId('...') and the string '...' share the same key.
KEY_FORMAT_LEGACY = 1
KEY_FORMAT_TYPED = 2


def _encode_key_legacy(key) -> str:
    return str(key)


def _encode_key_typed(key) -> str:  # noqa: PLR0911 - one return per type
    """Encode a key so that ids MongoDB considers different get different keys, while ids it
    considers equal (e.g. 1 and 1.0) get the same key"""
    if isinstance(key, str):
        return "s:" + key
    if isinstance(key, bson.ObjectId):
        return "o:" + str(key)
    if isinstance(key, bool):
        return "B:" + str(key)
    if isinstance(key, (int, float)):
        if isinstance(key, float) and key.is_integer():
            key = int(key)
        return "n:" + repr(key)
    if isinstance(key, bson.Decimal128):
        return _encode_key_typed(float(key.to_decimal()))
    if isinstance(key, uuid.UUID):
        return "u:" + str(key)
    if isinstance(key, (bytes, bson.Binary)):
        return "x:" + bytes(key).hex()
    if isinstance(key, datetime.datetime):
        return "t:" + key.isoformat()
    if key is None:
        return "N:"
    # e.g. mongomock's hashdict for embedded document ids
    return f"{type(key).__name__}:{key!r}"


def _is_collection_file(path: pathlib.Path) -> bool:
    return path.is_file() and not path.name.endswith(_SQLITE_SIDECAR_SUFFIXES)


class ServerStore(stores.ServerStore):
    def __init__(self, dirpath: Union[str, pathlib.Path] = ""):
        """
        Create a new server store

        :param dirpath: the directory where the database folders will be stored, defaults to
            current directory
        """
        super().__init__()
        self._dirpath = pathlib.Path(dirpath).absolute()
        self._databases = {}

        if self._dirpath.exists():
            if not self._dirpath.is_dir():
                raise ValueError(f"Path exists but is not a directory: {self._dirpath}")
        else:
            self._dirpath.mkdir(parents=True)

    def __getitem__(self, db_name) -> "DatabaseStore":
        if db_name not in self._databases:
            self._databases[db_name] = DatabaseStore(self._dirpath / db_name)

        return self._databases[db_name]

    def __contains__(self, db_name):
        if db_name not in self._databases and not (self._dirpath / db_name).is_dir():
            return False
        return self[db_name].is_created

    def list_created_database_names(self):
        # Include databases created by previous sessions that are on disk
        names = dict.fromkeys(self._databases)
        names.update(dict.fromkeys(path.name for path in self._dirpath.iterdir() if path.is_dir()))
        return [name for name in names if self[name].is_created]

    def close(self):
        for database in self._databases.values():
            database.close()


class DatabaseStore(mongomock_store.DatabaseStore):
    """Object holding the data for a database (many collections)."""

    def __init__(self, path: pathlib.Path, codec_options: bson.CodecOptions = None):
        self._dirpath = path.absolute()
        self._codec_options = codec_options or bson.DEFAULT_CODEC_OPTIONS
        self._collections: dict[str, CollectionStore] = {}

        if self._dirpath.exists():
            if not self._dirpath.is_dir():
                raise ValueError(f"Path exists but is not a directory: {self._dirpath}")
        else:
            self._dirpath.mkdir(parents=True)

    def set_codec_options(self, codec_options: bson.CodecOptions):
        self._codec_options = codec_options

    def __getitem__(self, col_name) -> "CollectionStore":
        if col_name not in self._collections:
            self._collections[col_name] = CollectionStore(
                self._dirpath / col_name, codec_options=self._codec_options
            )

        return self._collections[col_name]

    def __contains__(self, col_name) -> bool:
        if col_name not in self._collections and not _is_collection_file(self._dirpath / col_name):
            return False

        return self[col_name].is_created

    def _all_collection_names(self) -> list:
        # Include collections created by previous sessions that are on disk
        names = dict.fromkeys(self._collections)
        names.update(
            dict.fromkeys(
                path.name for path in self._dirpath.iterdir() if _is_collection_file(path)
            )
        )
        return list(names)

    def list_created_collection_names(self):
        return [name for name in self._all_collection_names() if self[name].is_created]

    def create_collection(self, name):
        col = self[name]
        col.create()
        return col

    def rename(self, name: str, new_name: str):
        # Files are never moved or deleted while in use as other connections (possibly in other
        # processes) may have them open.  Instead, the contents are moved between the files in
        # ordinary transactions, which other connections will see.
        source = self[name]
        target = self[new_name]
        target.copy_from(source)
        source.drop()

    @property
    def is_created(self):
        return any(self[coll_name].is_created for coll_name in self._all_collection_names())

    def close(self):
        for coll in self._collections.values():
            coll.close()
        self._collections.clear()


class CollectionStore(mongomock_store.CollectionStore):
    """Object holding the data for a collection."""

    DOCUMENTS = "documents"
    INDEXES = "indexes"
    INFO = "info"

    ID = "_id"
    DOC = "doc"

    _documents = None
    indexes = None
    _connection = None
    _cur = None

    def __init__(self, filename: pathlib.Path, codec_options: bson.CodecOptions = None):
        self._path = filename.absolute()
        self._codec_options = codec_options or bson.DEFAULT_CODEC_OPTIONS
        self._is_force_created = False
        self._ttl_indexes = {}
        self._query_index = None
        self._data_version = None

        self._rwlock = mongomock_thread.RWLock()
        self.open()

    def create(self):
        self._is_force_created = True
        # Remember this so that the collection still exists in later sessions even if empty
        with self._connection:
            self._connection.execute(
                f"INSERT OR REPLACE INTO {self.INFO} VALUES ('force_created', 1)"  # nosec
            )

    @property
    def is_created(self):
        return bool(not self.is_empty or self.indexes or self._is_force_created)

    @property
    def documents(self):
        self._remove_expired_documents()
        yield from self._documents.values()

    def __len__(self) -> int:
        return len(self._documents)

    def drop(self):
        # Empty the file rather than deleting it: other connections may have it open and they
        # need to see that the collection has gone
        with self._rwlock.writer(), self._connection:
            for table in (self.DOCUMENTS, self.INDEXES):
                self._connection.execute(f"DELETE FROM {table}")  # nosec
            # Keep the key format, it describes the file rather than the data
            self._connection.execute(f"DELETE FROM {self.INFO} WHERE key != 'key_format'")  # nosec
        self.indexes.invalidate()
        self._ttl_indexes.clear()
        self._is_force_created = False
        self._query_index = None

    def copy_from(self, source: "CollectionStore"):
        """Replace the contents of this collection with those of the source"""
        # The rows are copied as is, so the target has to use the same key format as the source
        self._documents.key_format = source._documents.key_format
        with self._rwlock.writer(), self._connection:
            for table in (self.DOCUMENTS, self.INDEXES, self.INFO):
                self._connection.execute(f"DELETE FROM {table}")  # nosec
                rows = source._connection.execute(
                    f"SELECT * FROM {table} ORDER BY ROWID"  # nosec
                ).fetchall()
                if rows:
                    placeholders = ",".join("?" * len(rows[0]))
                    query = f"INSERT INTO {table} VALUES ({placeholders})"  # nosec
                    self._connection.executemany(query, rows)
        self.indexes.invalidate()
        self._is_force_created = source._is_force_created
        self._ttl_indexes = dict(source._ttl_indexes)
        self._query_index = None

    def open(self):
        self._connection = sqlite3.connect(self._path)
        # Write-ahead logging makes commits much cheaper (no sync of the main database file on
        # every write) while keeping each write atomic.  With synchronous=NORMAL a power cut can
        # lose the most recent commits but can't corrupt the database.
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=NORMAL")
        self._cur = self._connection.cursor()
        key_format = self._init_info()
        self._documents = TableDict(
            self.DOCUMENTS,
            self._connection,
            self._cur,
            codec_options=self._codec_options,
            key_format=key_format,
        )
        self.indexes = IndexDict(
            self.INDEXES,
            self._connection,
            self._cur,
            codec_options=self._codec_options,
            key_format=key_format,
        )
        row = self._connection.execute(
            f"SELECT value FROM {self.INFO} WHERE key = 'force_created'"  # nosec
        ).fetchone()
        self._is_force_created = bool(row and row[0])
        # TTL indexes are only kept in memory by mongomock, so restore them from disk
        self._ttl_indexes = {
            name: index
            for name, index in self.indexes.items()
            if index.get("expireAfterSeconds") is not None
        }
        self._query_index = None
        self._data_version = None

    def _init_info(self) -> int:
        """Create the info table (if needed) and get the key format of this file"""
        with self._connection:
            self._connection.execute(
                f"CREATE TABLE IF NOT EXISTS {self.INFO}(key PRIMARY KEY, value)"  # nosec
            )
            row = self._connection.execute(
                f"SELECT value FROM {self.INFO} WHERE key = 'key_format'"  # nosec
            ).fetchone()
            if row is not None:
                return row[0]

            # No format recorded: if there is existing data it must be in the legacy format
            has_data = any(
                self._connection.execute(
                    f"SELECT 1 FROM sqlite_master WHERE type='table' AND name='{table}'"  # nosec
                ).fetchone()
                and self._connection.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()  # nosec
                for table in (self.DOCUMENTS, self.INDEXES)
            )
            key_format = KEY_FORMAT_LEGACY if has_data else KEY_FORMAT_TYPED
            self._connection.execute(
                f"INSERT INTO {self.INFO} VALUES ('key_format', ?)",  # nosec
                (key_format,),
            )
            return key_format

    def close(self):
        if self._connection is None:
            return
        self._cur.close()
        self._cur = None
        self._connection.close()
        self._connection = None
        self._documents = None
        self.indexes = None
        self._query_index = None

    # region Query planning hooks

    def _index_key(self, key):
        # Keys come back from the database encoded, so use the encoded form in the query index
        return self._documents.encode_key(key)

    def _iter_items(self):
        yield from self._documents.items()

    def _get_many(self, keys):
        # The query index holds keys that are already encoded
        yield from self._documents.get_many_encoded(keys)

    def _query_index_is_stale(self) -> bool:
        """Check if another connection has changed the database since we last looked"""
        data_version = self._connection.execute("PRAGMA data_version").fetchone()[0]
        if data_version != self._data_version:
            self._data_version = data_version
            return True
        return False

    # endregion


class TableDict(collections.abc.MutableMapping):
    ID = "_id"
    DOC = "doc"

    def __init__(
        self,
        table_name: str,
        connection: sqlite3.Connection,
        cursor: sqlite3.Cursor,
        codec_options: bson.CodecOptions = None,
        *,
        key_format: int = KEY_FORMAT_TYPED,
    ):
        self._table_name = table_name
        self._connection = connection
        self._cur = cursor
        self._codec_options = codec_options or bson.DEFAULT_CODEC_OPTIONS
        self.key_format = key_format
        self._init_table()

    def _init_table(self):
        with self._connection:
            self._cur.execute(
                f"CREATE TABLE IF NOT EXISTS {self._table_name}("  # nosec
                f"{self.ID} PRIMARY KEY,"
                f"{self.DOC}"
                f")"
            )

    def __len__(self) -> int:
        res = self._cur.execute(f"select count(*) from {self._table_name}")  # nosec
        counts = res.fetchone()
        return counts[0]

    def __bool__(self) -> bool:
        res = self._cur.execute(f"SELECT 1 FROM {self._table_name} LIMIT 1")  # nosec
        return res.fetchone() is not None

    def __iter__(self):
        cur = self._connection.cursor()
        try:
            for row in cur.execute(
                f"SELECT {self.ID} from {self._table_name} ORDER BY ROWID"  # nosec
            ):
                yield row[0]
        finally:
            cur.close()

    def __contains__(self, key) -> bool:
        res = self._cur.execute(
            f"SELECT 1 from {self._table_name} where {self.ID}=?",  # nosec
            (self.encode_key(key),),
        )
        return res.fetchone() is not None

    def __getitem__(self, key: str):
        key = self.encode_key(key)
        res = self._cur.execute(
            f"SELECT {self.DOC} from {self._table_name} where {self.ID}=?",  # nosec
            (key,),
        )
        value = res.fetchone()
        if value is None:
            raise KeyError(key)

        return self._decode_doc(value[0])

    def __setitem__(self, key, val: dict):
        key = self.encode_key(key)
        if not isinstance(val, bytes):
            val = self._encode_doc(val)
        with self._connection:
            self._cur.execute(
                f"INSERT INTO {self._table_name} VALUES(?, ?) "  # nosec
                f"ON CONFLICT({self.ID}) DO UPDATE SET {self.DOC}=excluded.{self.DOC}",
                (key, val),
            )

    def __delitem__(self, key):
        key = self.encode_key(key)
        with self._connection:
            res = self._cur.execute(
                f"DELETE FROM {self._table_name} WHERE {self.ID}=?",  # nosec
                (key,),
            )
        if res.rowcount <= 0:
            raise KeyError(key)

    def values(self) -> Iterator[dict]:
        cur = self._connection.cursor()
        try:
            for row in cur.execute(
                f"SELECT {self.DOC} from {self._table_name} ORDER BY ROWID"  # nosec
            ):
                yield self._decode_doc(row[0])
        finally:
            cur.close()

    def items(self) -> Iterator[tuple]:
        """Iterate over (encoded key, document) pairs in insertion order using a single query"""
        cur = self._connection.cursor()
        try:
            for row in cur.execute(
                f"SELECT {self.ID}, {self.DOC} from {self._table_name} ORDER BY ROWID"  # nosec
            ):
                yield row[0], self._decode_doc(row[1])
        finally:
            cur.close()

    def get_many(self, keys: Iterable) -> Iterator[dict]:
        """Get the documents with the given keys, in the order given, skipping missing ones"""
        yield from self.get_many_encoded([self.encode_key(key) for key in keys])

    def get_many_encoded(self, keys: list) -> Iterator[dict]:
        """Like get_many() but takes keys that have already been encoded"""
        for start in range(0, len(keys), _MAX_PARAMS):
            chunk = keys[start : start + _MAX_PARAMS]
            placeholders = ",".join("?" * len(chunk))
            found = dict(
                self._cur.execute(
                    f"SELECT {self.ID}, {self.DOC} FROM {self._table_name} "  # nosec
                    f"WHERE {self.ID} IN ({placeholders})",
                    chunk,
                ).fetchall()
            )
            for key in chunk:
                encoded = found.get(key)
                if encoded is not None:
                    yield self._decode_doc(encoded)

    def encode_key(self, key) -> str:
        if self.key_format == KEY_FORMAT_LEGACY:
            return _encode_key_legacy(key)
        return _encode_key_typed(key)

    def _encode_doc(self, doc: dict) -> Union[str, bytes]:
        """Encode the document dictionary for storing in the backend

        We have also tried bson.json_util.dumps(doc) but this adds a significant computational cost.

        :param doc: the document to be encoded
        :return: the encoded version that can be decoded back using _decode_doc()
        """
        check_keys = helpers.PYMONGO_BELOW_3_6
        return bson.encode(doc, check_keys=check_keys, codec_options=self._codec_options)

    def _decode_doc(self, encoded: Union[str, bytes]) -> dict:
        """
        Decode a document dictionary previously encoded using _encode_doc()

        :param encoded: the encoded document
        :return: the decoded document dictionary
        """
        return bson.decode(encoded, codec_options=self._codec_options)


class IndexDict(TableDict):
    """Specialised group dictionary that stores MongoDB index information.

    This is necessary because index key specifications are given as tuples while BSON which is used
    to store the index dictionary converts these to lists.  Here we intercept decoding and convert
    back to tuples.

    The index specifications are needed on every query, so they are cached.  The cache is
    invalidated on writes and, by the collection store, when another connection changes the file.
    """

    _cache = None
    _cache_data_version = None

    def encode_key(self, key) -> str:
        # Index names are always strings (so can't clash) and mongomock reads them back as keys
        return str(key)

    def invalidate(self):
        self._cache = None

    def _get_cache(self) -> dict:
        # Another connection may have changed the indexes (data_version only changes for commits
        # made by other connections, and is very cheap to read)
        data_version = self._connection.execute("PRAGMA data_version").fetchone()[0]
        if self._cache is None or data_version != self._cache_data_version:
            self._cache = dict(super().items())
            self._cache_data_version = data_version
        return self._cache

    def __len__(self) -> int:
        return len(self._get_cache())

    def __bool__(self) -> bool:
        return bool(self._get_cache())

    def __iter__(self):
        return iter(list(self._get_cache()))

    def __contains__(self, key) -> bool:
        return self.encode_key(key) in self._get_cache()

    def __getitem__(self, key):
        return self._get_cache()[self.encode_key(key)]

    def __setitem__(self, key, val: dict):
        super().__setitem__(key, val)
        self.invalidate()

    def __delitem__(self, key):
        try:
            super().__delitem__(key)
        finally:
            self.invalidate()

    def values(self):
        return list(self._get_cache().values())

    def items(self):
        return list(self._get_cache().items())

    def _decode_doc(self, encoded) -> dict:
        index_dict = super()._decode_doc(encoded)
        # Convert list of index keys to tuples as they should be
        index_dict["key"] = list(map(tuple, index_dict["key"]))
        return index_dict
