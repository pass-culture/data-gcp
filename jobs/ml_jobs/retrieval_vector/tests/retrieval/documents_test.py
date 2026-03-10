import json

import numpy as np
import pytest

from app.retrieval.documents import _FORMAT_VERSION, Document, DocumentArray


class TestDocument:
    def test_basic_creation(self):
        doc = Document(id="item_1", embedding=[0.1, 0.2, 0.3])
        assert doc.id == "item_1"
        assert doc.embedding == [0.1, 0.2, 0.3]

    def test_creation_no_id(self):
        doc = Document(embedding=[-0.0001])
        assert doc.id is None
        assert doc.embedding == [-0.0001]

    def test_extra_kwargs(self):
        doc = Document(id="x", embedding=[1.0], price=9.99, category="book")
        assert doc.price == 9.99
        assert doc.category == "book"

    def test_always_truthy(self):
        assert bool(Document())
        assert bool(Document(id=None, embedding=None))

    def test_repr(self):
        doc = Document(id="a", embedding=[1])
        assert "Document(id='a'" in repr(doc)

    def test_eq_same(self):
        a = Document(id="a", embedding=[1.0, 2.0])
        b = Document(id="a", embedding=[1.0, 2.0])
        assert a == b

    def test_eq_numpy(self):
        a = Document(id="a", embedding=np.array([1.0, 2.0]))
        b = Document(id="a", embedding=[1.0, 2.0])
        assert a == b

    def test_neq_different_id(self):
        a = Document(id="a", embedding=[1.0])
        b = Document(id="b", embedding=[1.0])
        assert a != b

    def test_neq_different_embedding(self):
        a = Document(id="a", embedding=[1.0])
        b = Document(id="a", embedding=[2.0])
        assert a != b

    def test_eq_not_implemented_for_other_types(self):
        doc = Document(id="a", embedding=[1.0])
        assert doc != "not a document"

    def test_to_dict_plain(self):
        doc = Document(id="x", embedding=[1.0, 2.0])
        d = doc._to_dict()
        assert d == {"id": "x", "embedding": [1.0, 2.0]}

    def test_to_dict_numpy(self):
        doc = Document(id="x", embedding=np.array([1.0, 2.0]))
        d = doc._to_dict()
        assert d == {"id": "x", "embedding": [1.0, 2.0]}

    def test_to_dict_with_extras(self):
        doc = Document(id="x", embedding=[1.0], price=np.float32(9.99))
        d = doc._to_dict()
        assert isinstance(d["price"], float)

    def test_from_dict_roundtrip(self):
        original = Document(id="item", embedding=[1.0, 2.0, 3.0], price=5.0)
        restored = Document._from_dict(original._to_dict())
        assert restored.id == original.id
        assert restored.embedding == original.embedding
        assert restored.price == original.price


class TestDocumentArray:
    def test_empty(self):
        da = DocumentArray()
        assert len(da) == 0

    def test_append_and_len(self):
        da = DocumentArray()
        da.append(Document(id="a", embedding=[1.0]))
        da.append(Document(id="b", embedding=[2.0]))
        assert len(da) == 2

    def test_contains(self):
        da = DocumentArray()
        da.append(Document(id="a", embedding=[1.0]))
        assert "a" in da
        assert "z" not in da

    def test_getitem(self):
        da = DocumentArray()
        da.append(Document(id="a", embedding=[1.0]))
        assert da["a"].embedding == [1.0]

    def test_getitem_missing_raises(self):
        da = DocumentArray()
        with pytest.raises(KeyError):
            _ = da["missing"]

    def test_iter(self):
        da = DocumentArray()
        da.append(Document(id="a", embedding=[1.0]))
        da.append(Document(id="b", embedding=[2.0]))
        ids = [doc.id for doc in da]
        assert ids == ["a", "b"]

    def test_init_with_docs(self):
        docs = [Document(id="a", embedding=[1.0]), Document(id="b", embedding=[2.0])]
        da = DocumentArray(docs)
        assert len(da) == 2
        assert "a" in da

    def test_repr(self):
        da = DocumentArray()
        da.append(Document(id="a", embedding=[1.0]))
        assert "1 docs" in repr(da)

    def test_overwrite_on_duplicate_id(self):
        da = DocumentArray()
        da.append(Document(id="a", embedding=[1.0]))
        da.append(Document(id="a", embedding=[999.0]))
        assert len(da) == 1
        assert da["a"].embedding == [999.0]


class TestDocumentArrayPersistence:
    def test_save_load_roundtrip(self, tmp_path):
        path = str(tmp_path / "test.docs")
        da = DocumentArray()
        da.append(Document(id="u1", embedding=[1.0, 2.0]))
        da.append(Document(id="u2", embedding=[3.0, 4.0]))
        da.save(path)

        loaded = DocumentArray.load(path)
        assert len(loaded) == 2
        assert "u1" in loaded
        assert loaded["u1"].embedding == [1.0, 2.0]
        assert loaded["u2"].embedding == [3.0, 4.0]

    def test_save_load_with_numpy_embeddings(self, tmp_path):
        path = str(tmp_path / "test.docs")
        da = DocumentArray()
        da.append(Document(id="a", embedding=np.array([1.0, 2.0, 3.0])))
        da.save(path)

        loaded = DocumentArray.load(path)
        assert loaded["a"].embedding == [1.0, 2.0, 3.0]

    def test_save_load_with_extra_attrs(self, tmp_path):
        path = str(tmp_path / "test.docs")
        da = DocumentArray()
        da.append(Document(id="a", embedding=[1.0], price=9.99, tags=["x", "y"]))
        da.save(path)

        loaded = DocumentArray.load(path)
        assert loaded["a"].price == 9.99
        assert loaded["a"].tags == ["x", "y"]

    def test_save_produces_valid_json(self, tmp_path):
        path = str(tmp_path / "test.docs")
        da = DocumentArray()
        da.append(Document(id="a", embedding=[1.0]))
        da.save(path)

        with open(path, "r") as fh:
            payload = json.load(fh)
        assert payload["_format_version"] == _FORMAT_VERSION
        assert len(payload["documents"]) == 1

    def test_load_missing_version_raises(self, tmp_path):
        path = str(tmp_path / "bad.docs")
        with open(path, "w") as fh:
            json.dump({"documents": []}, fh)

        with pytest.raises(ValueError, match="missing '_format_version'"):
            DocumentArray.load(path)

    def test_load_future_version_raises(self, tmp_path):
        path = str(tmp_path / "future.docs")
        with open(path, "w") as fh:
            json.dump({"_format_version": 9999, "documents": []}, fh)

        with pytest.raises(ValueError, match="format version 9999"):
            DocumentArray.load(path)

    def test_save_load_empty(self, tmp_path):
        path = str(tmp_path / "empty.docs")
        da = DocumentArray()
        da.save(path)

        loaded = DocumentArray.load(path)
        assert len(loaded) == 0

    def test_load_nonexistent_file_raises(self):
        with pytest.raises(FileNotFoundError):
            DocumentArray.load("/tmp/nonexistent_docs_file_12345.docs")
