"""
Language identification of book descriptions, with py3langid: pure Python,
deterministic, no model download. Short descriptions are inherently harder
to classify correctly -- a general limitation of language ID.

Used once on the full item table (data_loading.add_languages) to filter
items by language before sampling, and as a fallback by nlp_preprocessing
for samples without a `lang` column.
"""

from __future__ import annotations

import os
from concurrent.futures import ProcessPoolExecutor

# Below this many docs, process start-up costs more than it saves (~0.7 ms/doc).
_PARALLEL_MIN_DOCS = 50_000


def _classify(doc: str) -> str:
    import py3langid as langid

    return langid.classify(doc)[0]


def detect_languages(docs: list[str], n_jobs: int | None = None) -> list[str]:
    """ISO 639-1 code per description, via py3langid (fast, deterministic,
    no model download -- unlike the spaCy pipelines used downstream).
    Spread over `n_jobs` processes (default: all cores) for large inputs."""
    n_jobs = n_jobs or os.cpu_count() or 1
    if n_jobs == 1 or len(docs) < _PARALLEL_MIN_DOCS:
        return [_classify(d) for d in docs]
    with ProcessPoolExecutor(n_jobs) as pool:
        return list(pool.map(_classify, docs, chunksize=2_000))
