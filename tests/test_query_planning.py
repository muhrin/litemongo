"""Tests for the index based query planning and the persistence behaviour of the SQLite store"""

import datetime
import random
import uuid

from bson import Decimal128, ObjectId
import pymongo
import pytest

import litemongo
from litemongo._vendor.mongomock import filtering, indexing

ENGINES = ("memory", "sqlite")


@pytest.fixture(params=ENGINES)
def uri(request, tmp_path):
    if request.param == "memory":
        # Each test gets its own in-memory server
        return f"litemongo://?engine=memory#test-{uuid.uuid4().hex}"
    return f"litemongo://{tmp_path}/server#test"


@pytest.fixture
def database(uri):
    return litemongo.connect(uri).get_default_database()


NOW = datetime.datetime(2024, 5, 17, 12, 30, 15, 123000)
OID = ObjectId()

# Values chosen to exercise MongoDB's equality rules (e.g. 1 == 1.0, True != 1)
VALUES = [
    0,
    1,
    1.0,
    2.5,
    True,
    False,
    "1",
    "a",
    "b",
    None,
    OID,
    str(OID),
    NOW,
    Decimal128("1"),
    [1, "a"],
    [],
    [[1]],
    {"x": 1},
    {"y": "a"},
]


def _random_doc(rng: random.Random, idx: int) -> dict:
    doc = {"_id": idx}
    for field in ("a", "b"):
        if rng.random() < 0.8:
            doc[field] = rng.choice(VALUES)
    if rng.random() < 0.7:
        # Nested values, sometimes through an array
        nested = {"x": rng.choice(VALUES)}
        doc["n"] = [nested, {"x": rng.choice(VALUES)}] if rng.random() < 0.3 else nested
    return doc


def _random_condition(rng: random.Random):
    scalar = rng.choice(VALUES)
    kind = rng.random()
    if kind < 0.5:
        return scalar
    if kind < 0.7:
        return {"$eq": scalar}
    if kind < 0.9:
        return {"$in": rng.sample(VALUES, rng.randint(0, 4))}
    return {"$exists": True}


def _random_query(rng: random.Random) -> dict:
    fields = ["a", "b", "n.x", "_id"]
    query = {}
    for field in rng.sample(fields, rng.randint(1, 2)):
        query[field] = rng.choice([_random_condition(rng), rng.randint(0, 50)])
    if rng.random() < 0.3:
        query = {"$and": [query, {rng.choice(fields): _random_condition(rng)}]}
    return query


def test_planned_queries_match_full_scan(database):
    """The indexes must never change the result of a query"""
    rng = random.Random(1234)
    coll = database.coll
    coll.create_index("a")
    coll.create_index([("b", pymongo.ASCENDING), ("a", pymongo.DESCENDING)])
    coll.create_index("n.x")

    docs = [_random_doc(rng, idx) for idx in range(60)]
    coll.insert_many(docs)
    # Mix in some updates and deletes so the indexes have to be maintained
    for idx in rng.sample(range(60), 15):
        coll.update_one({"_id": idx}, {"$set": {"a": rng.choice(VALUES)}})
    for idx in rng.sample(range(60), 5):
        coll.delete_one({"_id": idx})

    all_docs = list(coll.find())
    for _ in range(400):
        query = _random_query(rng)
        expected = [doc["_id"] for doc in all_docs if filtering.filter_applies(query, doc)]
        found = [doc["_id"] for doc in coll.find(query)]
        assert found == expected, query
        # Aggregations starting with a $match use the indexes too
        aggregated = [doc["_id"] for doc in coll.aggregate([{"$match": query}])]
        assert aggregated == expected, query


def test_natural_order_is_kept(database):
    coll = database.coll
    coll.create_index("a")
    coll.insert_many([{"_id": idx, "a": idx % 2} for idx in range(10)])
    coll.update_one({"_id": 4}, {"$set": {"b": 1}})  # Updates keep their position
    coll.delete_one({"_id": 2})
    coll.insert_one({"_id": 2, "a": 0})  # Re-inserts go to the end
    assert [doc["_id"] for doc in coll.find({"a": 0})] == [0, 4, 6, 8, 2]


def test_index_created_after_insertion(database):
    coll = database.coll
    coll.insert_many([{"_id": idx, "a": idx % 3} for idx in range(10)])
    assert len(list(coll.find({"a": 1}))) == 3
    coll.create_index("a")
    assert [doc["_id"] for doc in coll.find({"a": 1})] == [1, 4, 7]
    coll.drop_index("a_1")
    assert [doc["_id"] for doc in coll.find({"a": 1})] == [1, 4, 7]
    coll.drop()
    assert not list(coll.find({"a": 1}))


def test_unique_index_uses_planner(database):
    coll = database.coll
    coll.create_index([("a", pymongo.ASCENDING), ("b", pymongo.ASCENDING)], unique=True)
    coll.insert_one({"a": 1, "b": 1})
    coll.insert_one({"a": 1, "b": 2})
    with pytest.raises(pymongo.errors.DuplicateKeyError):
        coll.insert_one({"a": 1.0, "b": 1})  # 1.0 == 1 in MongoDB
    assert coll.count_documents({}) == 2


def test_partial_unique_index(database):
    coll = database.coll
    # Documents without the field don't take part in a partial index...
    coll.insert_many([{"_id": 1}, {"_id": 2}])
    coll.create_index("reg", unique=True, partialFilterExpression={"reg": {"$exists": True}})
    coll.insert_one({"_id": 3})
    coll.update_one({"_id": 1}, {"$set": {"reg": "AB12"}})
    # ...but documents with it do
    with pytest.raises(pymongo.errors.DuplicateKeyError):
        coll.update_one({"_id": 2}, {"$set": {"reg": "AB12"}})
    with pytest.raises(pymongo.errors.DuplicateKeyError):
        coll.insert_one({"_id": 4, "reg": "AB12"})
    coll.insert_one({"_id": 5, "reg": "CD34"})


def test_key_function():
    # Values that MongoDB considers equal must have equal keys
    assert indexing.query_keys(1) == indexing.query_keys(1.0) == indexing.query_keys(Decimal128("1"))
    # MongoDB says True != 1, but mongomock's filtering says they are equal, so they must share keys
    assert indexing.query_keys(True) == indexing.query_keys(1)
    assert indexing.query_keys(OID) != indexing.query_keys(str(OID))
    # These can't be used for planning
    assert indexing.query_keys(None) is None
    assert indexing.query_keys([1]) is None
    assert indexing.query_keys({"$gt": 1}) is None
    assert indexing.query_keys({"$in": [1, None]}) is None


# region SQLite persistence


def test_sqlite_sees_changes_from_other_connections(tmp_path):
    uri = f"litemongo://{tmp_path}/server#test"
    coll1 = litemongo.connect(uri).get_default_database().coll
    coll2 = litemongo.connect(uri).get_default_database().coll
    coll1.create_index("a")
    coll1.insert_one({"_id": 1, "a": 1})
    assert coll2.find_one({"a": 1})["_id"] == 1  # Builds coll2's index

    # Change things via the first connection, the second one must notice
    coll1.update_one({"_id": 1}, {"$set": {"a": 2}})
    coll1.insert_one({"_id": 2, "a": 1})
    assert [doc["_id"] for doc in coll2.find({"a": 1})] == [2]
    assert [doc["_id"] for doc in coll2.find({"a": 2})] == [1]
    coll1.delete_one({"_id": 2})
    assert not list(coll2.find({"a": 1}))
    # Including new indexes
    coll1.create_index("b", unique=True)
    coll2.insert_one({"_id": 3, "b": 1})
    with pytest.raises(pymongo.errors.DuplicateKeyError):
        coll2.insert_one({"_id": 4, "b": 1})


def test_sqlite_reopen_lists_existing(tmp_path):
    uri = f"litemongo://{tmp_path}/server#test"
    database = litemongo.connect(uri).get_default_database()
    database.coll.insert_one({"a": 1})
    database.create_collection("empty")
    database.get_collection("untouched")  # Never written to, so doesn't exist

    client = litemongo.connect(uri)
    assert client.list_database_names() == ["test"]
    assert sorted(client.get_default_database().list_collection_names()) == ["coll", "empty"]


def test_sqlite_awkward_ids(tmp_path):
    coll = litemongo.connect(f"litemongo://{tmp_path}/server#test").get_default_database().coll
    for doc_id in ("it's", 'say "hi"', {"oid": 1, "v": 2}, "--", "a'; DROP TABLE documents; --"):
        coll.insert_one({"_id": doc_id, "value": 1})
        assert coll.find_one({"_id": doc_id})["value"] == 1
    assert coll.count_documents({}) == 5


def test_sqlite_rename_over_existing(tmp_path):
    database = litemongo.connect(f"litemongo://{tmp_path}/server#test").get_default_database()
    database.a.insert_one({"_id": 1})
    database.c.insert_one({"_id": 2})
    database.a.rename("c", dropTarget=True)
    assert database.list_collection_names() == ["c"]
    assert [doc["_id"] for doc in database.c.find()] == [1]


# endregion


def test_gridfs(database):
    """GridFS from recent pymongo versions reads client options that mongomock didn't provide"""
    import gridfs

    from litemongo._vendor.mongomock import gridfs as mongomock_gridfs

    mongomock_gridfs.enable_gridfs_integration()
    bucket = gridfs.GridFSBucket(database)
    file_id = bucket.upload_from_stream("test.txt", b"hello world")
    assert bucket.open_download_stream(file_id).read() == b"hello world"


def test_sqlite_drop_and_rename_seen_by_other_connections(tmp_path):
    uri = f"litemongo://{tmp_path}/server#test"
    db1 = litemongo.connect(uri).get_default_database()
    db2 = litemongo.connect(uri).get_default_database()
    db1.a.insert_one({"_id": 1, "x": 1})
    assert db2.a.find_one({"_id": 1})

    db1.a.rename("b")
    assert db2.a.find_one({"_id": 1}) is None
    assert db2.b.find_one({"_id": 1})["x"] == 1
    assert db2.list_collection_names() == ["b"]

    db1.b.drop()
    assert db2.b.find_one({"_id": 1}) is None
    assert db2.list_collection_names() == []
    # And the collection is usable again afterwards
    db2.b.insert_one({"_id": 2})
    assert [doc["_id"] for doc in db1.b.find()] == [2]


def test_sqlite_ids_of_different_types(tmp_path):
    """Ids that MongoDB considers different must not clash, ones it considers equal must"""
    coll = litemongo.connect(f"litemongo://{tmp_path}/server#test").get_default_database().coll
    oid = ObjectId()
    coll.insert_one({"_id": oid, "v": "objectid"})
    coll.insert_one({"_id": str(oid), "v": "string"})
    coll.insert_one({"_id": 1, "v": "int"})
    coll.insert_one({"_id": "1", "v": "string"})
    with pytest.raises(pymongo.errors.DuplicateKeyError):
        coll.insert_one({"_id": 1.0, "v": "float"})
    assert coll.find_one({"_id": oid})["v"] == "objectid"
    assert coll.find_one({"_id": str(oid)})["v"] == "string"
    assert coll.find_one({"_id": 1})["v"] == "int"
    assert coll.count_documents({}) == 4


def test_sqlite_legacy_key_format(tmp_path):
    """Files written before the key format was recorded must still be readable"""
    import sqlite3

    from litemongo import sqlite_store

    uri = f"litemongo://{tmp_path}/server#test"
    coll = litemongo.connect(uri).get_default_database().coll
    oid = ObjectId()
    coll.insert_one({"_id": oid, "v": 1})
    coll.create_index("v")

    # Turn the file into a legacy one: legacy keys and no recorded format
    path = tmp_path / "server" / "test" / "coll"
    conn = sqlite3.connect(path)
    with conn:
        conn.execute("UPDATE documents SET _id = ?", (str(oid),))
        conn.execute("DELETE FROM info WHERE key = 'key_format'")
    conn.close()

    coll = litemongo.connect(uri).get_default_database().coll
    assert coll.find_one({"_id": oid})["v"] == 1
    assert "v_1" in coll.index_information()
    coll.insert_one({"_id": ObjectId(), "v": 2})
    assert coll.count_documents({}) == 2
    store = coll.database._store["coll"]
    assert store._documents.key_format == sqlite_store.KEY_FORMAT_LEGACY

    # Renaming into a new file carries the format across
    coll.rename("renamed")
    renamed = litemongo.connect(uri).get_default_database().renamed
    assert renamed.find_one({"_id": oid})["v"] == 1
