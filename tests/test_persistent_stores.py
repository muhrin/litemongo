"""Tests that apply to all the stores that keep their data in a file"""

import uuid

import pymongo
import pymongo.errors
import pytest

import litemongo

ENGINES = ("sqlite", "h5py", "pytables")
MODULES = {"h5py": "h5py", "pytables": "tables"}


@pytest.fixture(params=ENGINES)
def uri(request, tmp_path):
    if request.param in MODULES:
        pytest.importorskip(MODULES[request.param])
    return f"litemongo://{tmp_path}/server?engine={request.param}#test"


def connect(uri):
    return litemongo.connect(uri, uuidRepresentation="standard").get_default_database()


def test_reopen(uri):
    """Documents and indexes are there when the file is opened again"""
    coll = connect(uri).coll
    coll.create_index("name", unique=True)
    coll.insert_many([{"name": "a"}, {"name": "b"}])

    coll = connect(uri).coll
    assert sorted(doc["name"] for doc in coll.find()) == ["a", "b"]
    assert "name_1" in coll.index_information()
    with pytest.raises(pymongo.errors.DuplicateKeyError):
        coll.insert_one({"name": "a"})


def test_codec_options(uri):
    """Documents are encoded using the client's options, e.g. how UUIDs are stored"""
    obj_id = uuid.uuid4()
    connect(uri).coll.insert_one({"_id": 1, "uuid": obj_id})

    assert connect(uri).coll.find_one({"uuid": obj_id}) == {"_id": 1, "uuid": obj_id}
