"""Hash indexes used to narrow down the documents a query has to look at.

This is a litemongo addition to mongomock.  Without it every query, update, delete and unique
check has to look at (and, for the persistent stores, decode) every document in the collection.

The indexes only ever produce a *superset* of the matching documents: each candidate is still
checked against the full filter by the usual mongomock machinery.  This means that the index keys
can be coarse (e.g. two different values sharing a key only costs some speed), but a document that
could match a query must never be left out.  To guarantee this:

* Documents whose value at an indexed path can't be reliably keyed (e.g. the path passes through
  an array, or the value is of an unknown type) are kept in an 'unkeyed' set that is returned as
  a candidate for every query on that path.
* Only simple query conditions are used for planning: equality with a scalar, ``$eq`` and ``$in``
  of scalars (combined with ``$and``).  Anything else, including ``None`` (which also matches
  missing fields), falls back to a full scan.
"""

import datetime
import decimal
import math
import re
import uuid
from typing import Any, Dict, Hashable, Iterable, Optional, Set

from bson import Binary, Decimal128, ObjectId, Regex

__all__ = ('CollectionIndexes', 'index_paths')

ID = '_id'

# Returned by _key_for() when a value has no reliable key
_NO_KEY = object()


def _key_for(value) -> Any:
    """Get the index key for a single (non-list) value, or _NO_KEY if it can't be keyed.

    Values that MongoDB considers equal must give equal keys."""
    # MongoDB never considers a bool equal to a number but mongomock's filtering (like Python) does,
    # so bools share keys with 0 and 1.  This keeps the candidates a superset of the matches either
    # way, and the final filtering decides.
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return ('nan',)
        # Numbers of different types compare equal in MongoDB (e.g. 1 == 1.0).  Large integers may
        # collide when converted to float, which is fine (superset).
        try:
            return ('n', float(value))
        except OverflowError:
            return ('n', 'big')
    if isinstance(value, Decimal128):
        return _key_for_decimal(value.to_decimal())
    if isinstance(value, decimal.Decimal):
        return _key_for_decimal(value)
    if isinstance(value, str):
        return ('s', value)
    if isinstance(value, ObjectId):
        return ('o', value)
    if isinstance(value, datetime.datetime):
        # Use whole seconds so that differences in sub-millisecond precision and timezone handling
        # can only ever make the key coarser
        if value.tzinfo is not None:
            value = value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return ('t', value.replace(microsecond=0))
    if isinstance(value, uuid.UUID):
        return ('x', value.bytes)
    if isinstance(value, (bytes, Binary)):
        return ('x', bytes(value))
    return _NO_KEY


def _key_for_decimal(value: decimal.Decimal):
    if value.is_nan():
        return ('nan',)
    try:
        return ('n', float(value))
    except (OverflowError, ValueError):
        return ('n', 'big')


def query_keys(condition) -> Optional[Set[Hashable]]:
    """Get the set of keys that a document must have (at least one of) to possibly match the given
    query condition on a field.  Returns None if the condition can't be used for planning."""
    if isinstance(condition, dict):
        if '$eq' in condition:
            return query_keys(condition['$eq']) if not isinstance(condition['$eq'], dict) else None
        if '$in' in condition:
            values = condition['$in']
            if not isinstance(values, (list, tuple, set)):
                return None
            keys = set()
            for value in values:
                if value is None or isinstance(value, (dict, list, tuple, re.Pattern, Regex)):
                    return None
                key = _key_for(value)
                if key is _NO_KEY:
                    return None
                keys.add(key)
            return keys
        return None

    if condition is None or isinstance(condition, (list, tuple, re.Pattern, Regex)):
        # None also matches missing fields, lists are matched as a whole, regexes match many values
        return None

    key = _key_for(condition)
    if key is _NO_KEY:
        return None
    return {key}


def _doc_keys(doc: dict, path: str):
    """Get the index keys for the value at the given path in a document.

    Returns a set of keys, or None if the document must be treated as matching any query on this
    path (because we can't reliably key it)."""
    value = doc
    for part in path.split('.'):
        if isinstance(value, dict):
            if part not in value:
                # A missing field can only match None (which we never plan with)
                return set()
            value = value[part]
        elif isinstance(value, (list, tuple)):
            # Paths that pass through arrays have complicated matching rules, so don't key them
            return None
        else:
            # Path continues through a scalar so there is no value here
            return set()

    if value is None:
        return set()
    if isinstance(value, dict):
        # An embedded document can't be equal to any of the scalars we plan with
        return set()
    if isinstance(value, (list, tuple)):
        # Arrays match if any element matches
        keys = set()
        for entry in value:
            if entry is None or isinstance(entry, (dict, list, tuple)):
                continue
            key = _key_for(entry)
            if key is _NO_KEY:
                return None
            keys.add(key)
        return keys

    key = _key_for(value)
    if key is _NO_KEY:
        return None
    return {key}


class FieldIndex:
    """A hash index on a single field path"""

    def __init__(self, path: str):
        self.path = path
        self._entries: Dict[Hashable, Set[Hashable]] = {}
        self._unkeyed: Set[Hashable] = set()
        self._doc_keys: Dict[Hashable, Any] = {}  # Store key -> the index keys for that doc

    def add(self, store_key: Hashable, doc: dict):
        keys = _doc_keys(doc, self.path)
        if keys is None:
            self._unkeyed.add(store_key)
        else:
            for key in keys:
                self._entries.setdefault(key, set()).add(store_key)
        self._doc_keys[store_key] = keys

    def remove(self, store_key: Hashable):
        try:
            keys = self._doc_keys.pop(store_key)
        except KeyError:
            return
        if keys is None:
            self._unkeyed.discard(store_key)
        else:
            for key in keys:
                entry = self._entries.get(key)
                if entry is not None:
                    entry.discard(store_key)
                    if not entry:
                        del self._entries[key]

    def candidates(self, keys: Iterable[Hashable]) -> Set[Hashable]:
        found = set(self._unkeyed)
        for key in keys:
            found.update(self._entries.get(key, ()))
        return found


class CollectionIndexes:
    """The set of hash indexes for a collection.

    There is always an index on _id, and one for each field that appears in a declared index."""

    def __init__(self):
        self._fields: Dict[str, FieldIndex] = {ID: FieldIndex(ID)}
        # Store key -> insertion sequence number, used to return candidates in natural order
        self._docs: Dict[Hashable, int] = {}
        self._next_seq = 0

    @property
    def paths(self) -> Set[str]:
        return set(self._fields)

    def clear(self):
        for path in list(self._fields):
            self._fields[path] = FieldIndex(path)
        self._docs.clear()
        self._next_seq = 0

    def set_paths(self, paths: Iterable[str], documents: Iterable[tuple]):
        """Make sure there are indexes on exactly the given paths (plus _id).  New indexes are
        populated from the passed (store key, document) pairs, which are only consumed if needed."""
        wanted = set(paths) | {ID}
        for path in set(self._fields) - wanted:
            del self._fields[path]
        new_paths = wanted - set(self._fields)
        if not new_paths:
            return
        new_indexes = [FieldIndex(path) for path in new_paths]
        for store_key, doc in documents:
            for index in new_indexes:
                index.add(store_key, doc)
        for index in new_indexes:
            self._fields[index.path] = index

    def add(self, store_key: Hashable, doc: dict):
        """Add (or replace) a document.  Replacing keeps the document's position in the natural
        order, just like updating an existing key in the stores does."""
        seq = self._docs.get(store_key)
        if seq is None:
            seq = self._next_seq
            self._next_seq += 1
        else:
            for index in self._fields.values():
                index.remove(store_key)
        for index in self._fields.values():
            index.add(store_key, doc)
        self._docs[store_key] = seq

    def remove(self, store_key: Hashable):
        if self._docs.pop(store_key, None) is None:
            return
        for index in self._fields.values():
            index.remove(store_key)

    def all_in_natural_order(self) -> list:
        """Get the store keys of all of the documents, in insertion order"""
        docs = self._docs
        return sorted(docs, key=docs.__getitem__)

    def in_natural_order(self, store_keys: Iterable[Hashable]) -> list:
        """Sort the given store keys into insertion order"""
        docs = self._docs
        return sorted((key for key in store_keys if key in docs), key=docs.__getitem__)

    def candidates(self, filter) -> Optional[Set[Hashable]]:  # pylint: disable=redefined-builtin
        """Get the store keys of the documents that could match the filter, or None if the filter
        can't be planned (and a full scan is needed)"""
        if not isinstance(filter, dict) or not filter:
            return None

        best = None
        for field, condition in filter.items():
            found = None
            if field == '$and' and isinstance(condition, (list, tuple)):
                for sub_filter in condition:
                    sub_found = self.candidates(sub_filter)
                    if sub_found is not None:
                        found = sub_found if found is None else found & sub_found
            elif field in self._fields:
                keys = query_keys(condition)
                if keys is not None:
                    found = self._fields[field].candidates(keys)

            if found is not None:
                best = found if best is None else best & found
                if not best:
                    return best

        return best


def index_paths(index_dicts: Iterable[dict]) -> Set[str]:
    """Get all the field paths used by the given index specifications"""
    paths = set()
    for index in index_dicts:
        for key in index.get('key', ()):
            name = key[0] if isinstance(key, (list, tuple)) else key
            if isinstance(name, str) and not name.startswith('$'):
                paths.add(name)
    return paths
