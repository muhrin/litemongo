import copy
import itertools
import math
from typing import Iterable

import mongomock
import mongomock.collection
import mongomock.filtering
import mongomock.helpers
import mongomock.not_implemented
import mongomock.store

from . import stores

__all__ = 'MongoClient', 'StoreType'


class StoreType:
    H5PY = 'h5py'
    PYTABLES = 'pytables'
    SQLITE = 'sqlite'
    MEMORY = 'memory'


def create_store(store_type=StoreType.SQLITE, *args, **kwargs) -> stores.ServerStore:
    """Server store factory"""
    if store_type == StoreType.H5PY:
        from . import h5py_store
        return h5py_store.ServerStore(*args, **kwargs)
    if store_type == StoreType.PYTABLES:
        from . import pytables_store
        return pytables_store.ServerStore(*args, **kwargs)
    if store_type == StoreType.SQLITE:
        from . import sqlite_store
        kwargs.pop('mode')
        return sqlite_store.ServerStore(*args, **kwargs)
    if store_type == StoreType.MEMORY:
        return mongomock.store.ServerStore()

    raise ValueError(f'Unknown store type: {store_type}')


class MongoClient(mongomock.MongoClient):
    def __init__(self, filename: str, mode='a', store_type=StoreType.SQLITE, **kwargs):
        server_store = create_store(store_type, filename, mode=mode)
        super().__init__(_store=server_store, **kwargs)

    def close(self):
        super().close()
        # Make sure to close the store
        self._store.close()


class Collection(mongomock.Collection):
    def _iter_documents(self, filter) -> Iterable[dict]:
        # Validate the filter even if no documents can be returned.
        if self._store.is_empty:
            mongomock.filtering.filter_applies(filter, {})

        return (document for document in self._store.documents
                if mongomock.filtering.filter_applies(filter, document))

    def count_documents(self, filter, **kwargs):
        if kwargs.pop('collation', None):
            mongomock.not_implemented.raise_for_feature(
                'collation',
                'The collation argument of count_documents is valid but has not been '
                'implemented in mongomock yet')
        if kwargs.pop('session', None):
            mongomock.not_implemented.raise_for_feature('session', 'Mongomock does not handle sessions yet')
        skip = kwargs.pop('skip', 0)
        if 'limit' in kwargs:
            limit = kwargs.pop('limit')
            if not isinstance(limit, (int, float)):
                raise mongomock.OperationFailure('the limit must be specified as a number')
            if limit <= 0:
                raise mongomock.OperationFailure('the limit must be positive')
            limit = math.floor(limit)
        else:
            limit = None
        unknown_kwargs = set(kwargs) - {'maxTimeMS', 'hint'}
        if unknown_kwargs:
            raise mongomock.OperationFailure("unrecognized field '%s'" % unknown_kwargs.pop())

        spec = mongomock.helpers.patch_datetime_awareness_in_document(filter)
        doc_num = mongomock.helpers.count_iter(self._iter_documents(spec))
        count = max(doc_num - skip, 0)
        return count if limit is None else min(count, limit)

    def _update(self, spec, document, upsert=False, manipulate=False,
                multi=False, check_keys=False, hint=None, session=None,
                collation=None, let=None, array_filters=None, **kwargs):
        if session:
            mongomock.not_implemented.raise_for_feature('session', 'Mongomock does not handle sessions yet')
        if hint:
            raise NotImplementedError(
                'The hint argument of update is valid but has not been implemented in '
                'mongomock yet')
        if collation:
            mongomock.not_implemented.raise_for_feature(
                'collation',
                'The collation argument of update is valid but has not been implemented in '
                'mongomock yet')
        if array_filters:
            mongomock.not_implemented.raise_for_feature(
                'array_filters', 'Array filters are not implemented in mongomock yet.')
        if let:
            mongomock.not_implemented.raise_for_feature(
                'let',
                'The let argument of update is valid but has not been implemented in mongomock '
                'yet')
        spec = mongomock.helpers.patch_datetime_awareness_in_document(spec)
        document = mongomock.helpers.patch_datetime_awareness_in_document(document)
        mongomock.collection.validate_is_mapping('spec', spec)
        mongomock.collection.validate_is_mapping('document', document)

        if self.database.client.server_info()['versionArray'] < [5]:
            for operator in mongomock.collection._updaters:
                if not document.get(operator, True):
                    raise mongomock.WriteError(
                        "'%s' is empty. You must specify a field like so: {%s: {<field>: ...}}"
                        % (operator, operator),
                    )

        updated_existing = False
        upserted_id = None
        num_updated = 0
        num_matched = 0
        for existing_document in itertools.chain(list(self._iter_documents(spec)), [None]):
            # we need was_insert for the setOnInsert update operation
            was_insert = False
            # the sentinel document means we should do an upsert
            if existing_document is None:
                if not upsert or num_matched:
                    continue
                # For upsert operation we have first to create a fake existing_document,
                # update it like a regular one, then finally insert it
                if spec.get('_id') is not None:
                    _id = spec['_id']
                elif document.get('_id') is not None:
                    _id = document['_id']
                else:
                    _id = mongomock.ObjectId()
                to_insert = dict(spec, _id=_id)
                to_insert = self._expand_dots(to_insert)
                to_insert, _ = self._discard_operators(to_insert)
                existing_document = to_insert
                was_insert = True
            else:
                original_document_snapshot = copy.deepcopy(existing_document)
                updated_existing = True
            num_matched += 1
            first = True
            subdocument = None
            for k, v in document.items():
                if k in mongomock.collection._updaters:
                    updater = mongomock.collection._updaters[k]
                    subdocument = self._update_document_fields_with_positional_awareness(
                        existing_document, v, spec, updater, subdocument)

                elif k == '$rename':
                    for src, dst in v.items():
                        if '.' in src or '.' in dst:
                            raise NotImplementedError(
                                'Using the $rename operator with dots is a valid MongoDB '
                                'operation, but it is not yet supported by mongomock'
                            )
                        if self._has_key(existing_document, src):
                            existing_document[dst] = existing_document.pop(src)

                elif k == '$setOnInsert':
                    if not was_insert:
                        continue
                    subdocument = self._update_document_fields_with_positional_awareness(
                        existing_document, v, spec, mongomock.collection._set_updater, subdocument)

                elif k == '$currentDate':
                    subdocument = self._update_document_fields_with_positional_awareness(
                        existing_document, v, spec, mongomock.collection._current_date_updater, subdocument)

                elif k == '$addToSet':
                    for field, value in v.items():
                        nested_field_list = field.rsplit('.')
                        if len(nested_field_list) == 1:
                            if field not in existing_document:
                                existing_document[field] = []
                            # document should be a list append to it
                            if isinstance(value, dict):
                                if '$each' in value:
                                    # append the list to the field
                                    existing_document[field] += [
                                        obj for obj in list(value['$each'])
                                        if obj not in existing_document[field]]
                                    continue
                            if value not in existing_document[field]:
                                existing_document[field].append(value)
                            continue
                        # push to array in a nested attribute
                        else:
                            # create nested attributes if they do not exist
                            subdocument = existing_document
                            for field_part in nested_field_list[:-1]:
                                if field_part == '$':
                                    break
                                if field_part not in subdocument:
                                    subdocument[field_part] = {}

                                subdocument = subdocument[field_part]

                            # get subdocument with $ oprator support
                            subdocument, _ = self._get_subdocument(
                                existing_document, spec, nested_field_list)

                            # we're pushing a list
                            push_results = []
                            if nested_field_list[-1] in subdocument:
                                # if the list exists, then use that list
                                push_results = subdocument[
                                    nested_field_list[-1]]

                            if isinstance(value, dict) and '$each' in value:
                                push_results += [
                                    obj for obj in list(value['$each'])
                                    if obj not in push_results]
                            elif value not in push_results:
                                push_results.append(value)

                            subdocument[nested_field_list[-1]] = push_results
                elif k == '$pull':
                    for field, value in v.items():
                        nested_field_list = field.rsplit('.')
                        # nested fields includes a positional element
                        # need to find that element
                        if '$' in nested_field_list:
                            if not subdocument:
                                subdocument, _ = self._get_subdocument(
                                    existing_document, spec, nested_field_list)

                            # value should be a dictionary since we're pulling
                            pull_results = []
                            # and the last subdoc should be an array
                            for obj in subdocument[nested_field_list[-1]]:
                                if isinstance(obj, dict):
                                    for pull_key, pull_value in value.items():
                                        if obj[pull_key] != pull_value:
                                            pull_results.append(obj)
                                    continue
                                if obj != value:
                                    pull_results.append(obj)

                            # cannot write to doc directly as it doesn't save to
                            # existing_document
                            subdocument[nested_field_list[-1]] = pull_results
                        else:
                            arr = existing_document
                            for field_part in nested_field_list:
                                if field_part not in arr:
                                    break
                                arr = arr[field_part]
                            if not isinstance(arr, list):
                                continue

                            arr_copy = copy.deepcopy(arr)
                            if isinstance(value, dict):
                                for obj in arr_copy:
                                    try:
                                        is_matching = mongomock.filter_applies(value, obj)
                                    except mongomock.OperationFailure:
                                        is_matching = False
                                    if is_matching:
                                        arr.remove(obj)
                                        continue

                                    if mongomock.filtering.filter_applies({'field': value}, {'field': obj}):
                                        arr.remove(obj)
                            else:
                                for obj in arr_copy:
                                    if value == obj:
                                        arr.remove(obj)
                elif k == '$pullAll':
                    for field, value in v.items():
                        nested_field_list = field.rsplit('.')
                        if len(nested_field_list) == 1:
                            if field in existing_document:
                                arr = existing_document[field]
                                existing_document[field] = [
                                    obj for obj in arr if obj not in value]
                            continue
                        else:
                            subdocument, _ = self._get_subdocument(
                                existing_document, spec, nested_field_list)

                            if nested_field_list[-1] in subdocument:
                                arr = subdocument[nested_field_list[-1]]
                                subdocument[nested_field_list[-1]] = [
                                    obj for obj in arr if obj not in value]
                elif k == '$push':
                    for field, value in v.items():
                        # Find the place where to push.
                        nested_field_list = field.rsplit('.')
                        subdocument, field = self._get_subdocument(
                            existing_document, spec, nested_field_list)

                        # Push the new element or elements.
                        if isinstance(subdocument, dict) and field not in subdocument:
                            subdocument[field] = []
                        push_results = subdocument[field]
                        if isinstance(value, dict) and '$each' in value:
                            if '$position' in value:
                                push_results = \
                                    push_results[0:value['$position']] + \
                                    list(value['$each']) + \
                                    push_results[value['$position']:]
                            else:
                                push_results += list(value['$each'])

                            if '$sort' in value:
                                sort_spec = value['$sort']
                                if isinstance(sort_spec, dict):
                                    sort_key = set(sort_spec.keys()).pop()
                                    push_results = sorted(
                                        push_results,
                                        key=lambda d: mongomock.helpers.get_value_by_dot(d, sort_key),
                                        reverse=set(sort_spec.values()).pop() < 0)
                                else:
                                    push_results = sorted(push_results, reverse=sort_spec < 0)

                            if '$slice' in value:
                                slice_value = value['$slice']
                                if slice_value < 0:
                                    push_results = push_results[slice_value:]
                                elif slice_value == 0:
                                    push_results = []
                                else:
                                    push_results = push_results[:slice_value]

                            unused_modifiers = \
                                set(value.keys()) - {'$each', '$slice', '$position', '$sort'}
                            if unused_modifiers:
                                raise mongomock.WriteError(
                                    'Unrecognized clause in $push: ' + unused_modifiers.pop())
                        else:
                            push_results.append(value)
                        subdocument[field] = push_results
                else:
                    if first:
                        # replace entire document
                        for key in document.keys():
                            if key.startswith('$'):
                                # can't mix modifiers with non-modifiers in
                                # update
                                raise ValueError('field names cannot start with $ [{}]'.format(k))
                        _id = spec.get('_id', existing_document.get('_id'))
                        existing_document.clear()
                        if _id is not None:
                            existing_document['_id'] = _id
                        if mongomock.collection.BSON:
                            # bson validation
                            mongomock.collection.BSON.encode(document, check_keys=True)
                        existing_document.update(self._internalize_dict(document))
                        if existing_document['_id'] != _id:
                            raise mongomock.OperationFailure(
                                'The _id field cannot be changed from {0} to {1}'
                                .format(existing_document['_id'], _id))
                        break
                    else:
                        # can't mix modifiers with non-modifiers in update
                        raise ValueError(
                            'Invalid modifier specified: {}'.format(k))
                first = False
            # if empty document comes
            if not document:
                _id = spec.get('_id', existing_document.get('_id'))
                existing_document.clear()
                if _id:
                    existing_document['_id'] = _id

            if was_insert:
                upserted_id = self._insert(existing_document)
                num_updated += 1
            elif existing_document != original_document_snapshot:
                # Document has been modified in-place.

                # Make sure the ID was not change.
                if original_document_snapshot.get('_id') != existing_document.get('_id'):
                    # Rollback.
                    self._store[original_document_snapshot['_id']] = original_document_snapshot
                    raise mongomock.WriteError(
                        "After applying the update, the (immutable) field '_id' was found to have "
                        'been altered to _id: {}'.format(existing_document.get('_id')))

                # Make sure it still respect the unique indexes and, if not, to
                # revert modifications
                try:
                    # Save the updated document in the store as the store may have provided a copy of the
                    # document rather than a dict that is being mutated in place.
                    self._store[existing_document['_id']] = existing_document
                    self._ensure_uniques(existing_document)
                    num_updated += 1
                except mongomock.DuplicateKeyError:
                    # Rollback.
                    self._store[original_document_snapshot['_id']] = original_document_snapshot
                    raise

            if not multi:
                break

        return {
            'connectionId': self.database.client._id,
            'err': None,
            'n': num_matched,
            'nModified': num_updated if updated_existing else 0,
            'ok': 1,
            'upserted': upserted_id,
            'updatedExisting': updated_existing,
        }


def count_iter(iterable: Iterable) -> int:
    """
    Count the number of entries in an iterable.
    Note, that this will consume the iterable.

    :param iterable: the iterable to count
    :return: the number of terms in the iterable
    """
    return sum(1 for _ in iterable)
