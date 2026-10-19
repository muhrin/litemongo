import collections
import datetime
import functools

from .. import mongomock
from . import indexing
from .thread import RWLock


class ServerStore:
    """Object holding the data for a whole server (many databases)."""

    def __init__(self):
        self._databases = {}

    def __getitem__(self, db_name):
        try:
            return self._databases[db_name]
        except KeyError:
            db = self._databases[db_name] = DatabaseStore()
            return db

    def __contains__(self, db_name):
        return self[db_name].is_created

    def list_created_database_names(self):
        return [name for name, db in self._databases.items() if db.is_created]


class DatabaseStore:
    """Object holding the data for a database (many collections)."""

    def __init__(self):
        self._collections = {}

    def __getitem__(self, col_name):
        try:
            return self._collections[col_name]
        except KeyError:
            col = self._collections[col_name] = CollectionStore(col_name)
            return col

    def __contains__(self, col_name):
        return self[col_name].is_created

    def list_created_collection_names(self):
        return [name for name, col in self._collections.items() if col.is_created]

    def create_collection(self, name):
        col = self[name]
        col.create()
        return col

    def rename(self, name, new_name):
        col = self._collections.pop(name, CollectionStore(new_name))
        col.name = new_name
        self._collections[new_name] = col

    @property
    def is_created(self):
        return any(col.is_created for col in self._collections.values())


class CollectionStore:
    """Object holding the data for a collection."""

    # litemongo: hash indexes used for query planning (see indexing.py), built lazily
    _query_index = None

    def __init__(self, name):
        self._documents = collections.OrderedDict()
        self.indexes = {}
        self._is_force_created = False
        self.name = name
        self._ttl_indexes = {}
        # litemongo: hash indexes used for query planning, built lazily
        self._query_index = None

        # 694 - Lock for safely iterating and mutating OrderedDicts
        self._rwlock = RWLock()

    def create(self):
        self._is_force_created = True

    @property
    def is_created(self):
        return self._documents or self.indexes or self._is_force_created

    def drop(self):
        self._documents = collections.OrderedDict()
        self.indexes = {}
        self._ttl_indexes = {}
        self._is_force_created = False
        self._query_index = None

    def create_index(self, index_name, index_dict):
        self.indexes[index_name] = index_dict
        if index_dict.get('expireAfterSeconds') is not None:
            self._ttl_indexes[index_name] = index_dict

    def drop_index(self, index_name):
        self._remove_expired_documents()

        # The main index object should raise a KeyError, but the
        # TTL indexes have no meaning to the outside.
        del self.indexes[index_name]
        self._ttl_indexes.pop(index_name, None)

    @property
    def is_empty(self):
        self._remove_expired_documents()
        return not self._documents

    def __contains__(self, key):
        self._remove_expired_documents()
        with self._rwlock.reader():
            return key in self._documents

    def __getitem__(self, key):
        self._remove_expired_documents()
        with self._rwlock.reader():
            return self._documents[key]

    def __setitem__(self, key, val):
        with self._rwlock.writer():
            self._documents[key] = val
            if self._query_index is not None:
                self._query_index.add(self._index_key(key), val)

    def __delitem__(self, key):
        with self._rwlock.writer():
            del self._documents[key]
            if self._query_index is not None:
                self._query_index.remove(self._index_key(key))

    def __len__(self):
        self._remove_expired_documents()
        with self._rwlock.reader():
            return len(self._documents)

    @property
    def documents(self):
        self._remove_expired_documents()
        with self._rwlock.reader():
            yield from self._documents.values()

    # region litemongo query planning

    def _index_key(self, key):
        """The key used for a document in the query index (stores may encode their keys)"""
        return key

    def _iter_items(self):
        """Iterate over (store key, document) pairs in natural order"""
        with self._rwlock.reader():
            yield from list(self._documents.items())

    def _query_index_is_stale(self) -> bool:
        """Persistent stores override this to report if the data was changed elsewhere"""
        return False

    def _get_query_index(self) -> indexing.CollectionIndexes:
        # Always check staleness so that persistent stores can track what they've seen
        stale = self._query_index_is_stale()
        if self._query_index is None or stale:
            query_index = indexing.CollectionIndexes()
            query_index.set_paths(indexing.index_paths(self.indexes.values()), ())
            for key, doc in self._iter_items():
                query_index.add(key, doc)
            self._query_index = query_index
        else:
            # Keep the indexed paths in line with the declared indexes
            self._query_index.set_paths(
                indexing.index_paths(self.indexes.values()), self._iter_items()
            )
        return self._query_index

    def find_candidates(self, filter):  # pylint: disable=redefined-builtin
        """Get the documents (in natural order) that could match the given filter.  The caller
        must still check each document against the filter."""
        self._remove_expired_documents()
        query_index = self._get_query_index()
        keys = query_index.candidates(filter)
        if keys is None:
            return self.documents
        return self._get_many(query_index.in_natural_order(keys))

    def _get_many(self, keys):
        with self._rwlock.reader():
            for key in keys:
                try:
                    yield self._documents[key]
                except KeyError:
                    pass

    # endregion

    def _remove_expired_documents(self):
        for index in self._ttl_indexes.values():
            self._expire_documents(index)

    def _expire_documents(self, index):
        # TODO(juannyg): use a caching mechanism to avoid re-expiring the documents if
        # we just did and no document was added / updated

        # Ignore non-integer values
        try:
            expiry = int(index['expireAfterSeconds'])
        except ValueError:
            return

        # Ignore commpound keys
        if len(index['key']) > 1:
            return

        # "key" structure = list of (field name, direction) tuples
        ttl_field_name = next(iter(index['key']))[0]
        ttl_now = mongomock.utcnow()

        with self._rwlock.reader():
            expired_ids = [
                doc['_id']
                for doc in self._documents.values()
                if self._value_meets_expiry(doc.get(ttl_field_name), expiry, ttl_now)
            ]

        for exp_id in expired_ids:
            del self[exp_id]

    def _value_meets_expiry(self, val, expiry, ttl_now):
        val_to_compare = _get_min_datetime_from_value(val)
        try:
            return (ttl_now - val_to_compare).total_seconds() >= expiry
        except TypeError:
            return False


def _get_min_datetime_from_value(val):
    if not val:
        return datetime.datetime.max
    if isinstance(val, list):
        return functools.reduce(_min_dt, [datetime.datetime.max, *val])
    return val


def _min_dt(dt1, dt2):
    try:
        return dt1 if dt1 < dt2 else dt2
    except TypeError:
        return dt1
