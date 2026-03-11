"""Lightweight Document / DocumentArray primitives.

These replace the docarray 0.21 ``Document`` / ``DocumentArray`` classes whose API
was completely rewritten in docarray 0.40+.  The codebase only needs:

* ``Document``      – a thin value-object carrying an ``id`` and an ``embedding``
                      (plus any extra keyword attributes stored dynamically).
* ``DocumentArray`` – an id-keyed collection with O(1) lookup, ``save`` / ``load``
                      via JSON, and the same container protocol as before.

The on-disk format changes from docarray's binary format to JSON, so any
``.docs`` files produced by the old version must be regenerated after this
migration (re-run ``create_vector_database.py``).
"""

from __future__ import annotations

import json
from typing import Any, Iterator, Optional

import numpy as np

# Bump this whenever the serialisation format changes so that stale files are
# detected early instead of producing cryptic errors at runtime.
_FORMAT_VERSION = 1


def _to_serializable(value: Any) -> Any:
    """Convert numpy arrays / scalars to plain Python types for JSON."""
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, (np.floating, np.integer)):
        return value.item()
    return value


class Document:
    """Minimal document holding an id and an embedding vector.

    Accepts arbitrary extra keyword arguments and stores them as instance
    attributes, preserving the flexible attribute access that was available
    with docarray 0.21 ``Document``.

    Examples
    --------
    >>> doc = Document(id="item_1", embedding=[0.1, 0.2, 0.3])
    >>> doc.embedding
    [0.1, 0.2, 0.3]
    >>> doc = Document(embedding=[-0.0001])        # query vector with no id
    """

    def __init__(
        self,
        id: Optional[str] = None,
        embedding: Any = None,
        **kwargs: Any,
    ) -> None:
        self.id = id
        self.embedding = embedding
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __bool__(self) -> bool:
        # Mimic docarray behaviour: a Document is always truthy.
        return True

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Document):
            return NotImplemented
        return self.id == other.id and np.array_equal(
            np.asarray(self.embedding), np.asarray(other.embedding)
        )

    def __repr__(self) -> str:
        return f"Document(id={self.id!r}, embedding={self.embedding!r})"

    def _to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe dict of all public attributes."""
        return {
            k: _to_serializable(v)
            for k, v in self.__dict__.items()
            if not k.startswith("_")
        }

    @classmethod
    def _from_dict(cls, data: dict[str, Any]) -> "Document":
        """Reconstruct a :class:`Document` from a dict produced by :meth:`_to_dict`."""
        return cls(**data)


class DocumentArray:
    """Id-keyed collection of :class:`Document` objects.

    Provides the same interface that was used against docarray 0.21
    ``DocumentArray``:

    * ``append(doc)``
    * ``doc_id in array``
    * ``array[doc_id]``
    * ``len(array)``
    * ``iter(array)``
    * ``array.save(path)``  /  ``DocumentArray.load(path)``

    Documents are stored in an ordered dict keyed by ``Document.id``.

    Examples
    --------
    >>> da = DocumentArray()
    >>> da.append(Document(id="u1", embedding=[1.0, 2.0]))
    >>> "u1" in da
    True
    >>> da["u1"].embedding
    [1.0, 2.0]
    """

    def __init__(self, docs=None) -> None:
        self._docs: dict[str, Document] = {}
        if docs is not None:
            for doc in docs:
                self.append(doc)

    # ------------------------------------------------------------------
    # Container protocol
    # ------------------------------------------------------------------

    def append(self, doc: Document) -> None:
        """Add *doc* to the array, keyed by ``doc.id``."""
        self._docs[doc.id] = doc

    def __contains__(self, doc_id: str) -> bool:
        return doc_id in self._docs

    def __getitem__(self, doc_id: str) -> Document:
        return self._docs[doc_id]

    def __len__(self) -> int:
        return len(self._docs)

    def __iter__(self) -> Iterator[Document]:
        return iter(self._docs.values())

    def __repr__(self) -> str:
        return f"DocumentArray({len(self)} docs)"

    # ------------------------------------------------------------------
    # Persistence (JSON-based, safe against arbitrary code execution)
    # ------------------------------------------------------------------

    def save(self, path: str) -> None:
        """Serialise the array to *path* as JSON."""
        payload = {
            "_format_version": _FORMAT_VERSION,
            "documents": [doc._to_dict() for doc in self._docs.values()],
        }
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh)

    @classmethod
    def load(cls, path: str) -> "DocumentArray":
        """Deserialise a :class:`DocumentArray` previously saved with :meth:`save`.

        Raises
        ------
        ValueError
            If the file has no ``_format_version`` key or the version is
            higher than the one supported by this code (stale/incompatible
            ``.docs`` file).
        """
        with open(path, "r", encoding="utf-8") as fh:
            payload = json.load(fh)

        version = payload.get("_format_version")
        if version is None:
            raise ValueError(
                f"File {path!r} is missing '_format_version'. "
                "It was likely produced by an older serialisation format. "
                "Please regenerate it with create_vector_database.py."
            )
        if version > _FORMAT_VERSION:
            raise ValueError(
                f"File {path!r} has format version {version}, but this code "
                f"only supports up to version {_FORMAT_VERSION}. "
                "Please upgrade the retrieval_vector package."
            )

        da = cls()
        for doc_dict in payload["documents"]:
            da.append(Document._from_dict(doc_dict))
        return da
