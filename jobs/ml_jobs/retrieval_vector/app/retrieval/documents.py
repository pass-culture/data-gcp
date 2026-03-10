"""
Lightweight Document / DocumentArray primitives.

These replace the docarray 0.21 ``Document`` / ``DocumentArray`` classes whose API
was completely rewritten in docarray 0.40+.  The codebase only needs:

* ``Document``      – a thin value-object carrying an ``id`` and an ``embedding``
                      (plus any extra keyword attributes stored dynamically).
* ``DocumentArray`` – an id-keyed collection with O(1) lookup, ``save`` / ``load``
                      via pickle, and the same container protocol as before.

The on-disk format changes from docarray's binary format to pickle, so any
``.docs`` files produced by the old version must be regenerated after this
migration (re-run ``create_vector_database.py``).
"""

from __future__ import annotations

import pickle
from typing import Any, Iterator, Optional


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

    def __repr__(self) -> str:
        return f"Document(id={self.id!r}, embedding={self.embedding!r})"


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
    # Persistence
    # ------------------------------------------------------------------

    def save(self, path: str) -> None:
        """Serialise the array to *path* using pickle."""
        with open(path, "wb") as fh:
            pickle.dump(self._docs, fh)

    @classmethod
    def load(cls, path: str) -> "DocumentArray":
        """Deserialise a :class:`DocumentArray` previously saved with :meth:`save`."""
        da = cls()
        with open(path, "rb") as fh:
            da._docs = pickle.load(fh)
        return da
