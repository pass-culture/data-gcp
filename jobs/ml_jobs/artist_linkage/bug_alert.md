# Bug: `match_artists_with_wikidata`'s `None` default crashes instead of working

**File:** `src/linkage/matching.py`
**Function:** `match_artists_with_wikidata` (starts at line 275)
**Status:** Not fixed. Discovered while writing `tests/linkage/matching_test.py`
(see `TestMatchArtistsWithWikidata::test_default_none_artist_with_wiki_ids_df_raises_instead_of_working`).

## What the function is supposed to do

`match_artists_with_wikidata` takes newly-formed artist clusters, matches each
against a Wikidata dump, and assigns each cluster a stable `artist_id` —
reusing an existing ID if that Wikidata entity is already known, or minting a
fresh one otherwise.

Its `artist_with_wiki_ids_df` parameter — "artists we already have a Wikidata
ID for" — defaults to `None`, and the docstring says:

> If none, we assume no existing artists are present.

So `None` is documented as a valid input meaning "starting from scratch."

## What actually happens

The `None`-handling fallback, at `src/linkage/matching.py:307-310`:

```python
if artist_with_wiki_ids_df is None:
    artist_with_wiki_ids_df = pd.DataFrame(
        columns=[ARTIST_ID_KEY, ARTIST_WIKI_ID_KEY]
    )
```

builds an empty frame with columns `artist_id` and **`artist_wiki_id`**.

A few lines later, at `src/linkage/matching.py:319`, the function does:

```python
set(artist_with_wiki_ids_df[WIKIDATA_ID_KEY])
```

which looks for a column named **`wikidata_id`** (the value of `WIKIDATA_ID_KEY`
in `src/common/constants.py`). That column was never created — the fallback
built `artist_wiki_id` (`ARTIST_WIKI_ID_KEY`), a different constant with a
similarly-named but distinct string value.

**Result:** calling the function with `artist_with_wiki_ids_df=None` — the
exact input the docstring documents as valid — raises `KeyError: 'wikidata_id'`
immediately, instead of behaving as "no existing artists are present."

## Why production hasn't hit this

The function has exactly one caller, `cli/link_new_products_to_artists.py:253`,
and it never passes `None` — it always builds its own real frame (with the
correct `wikidata_id` column, taken from the artists table) before calling in.
The broken `None` branch is simply never exercised today. It would surface the
moment:
- a from-scratch run has no prior artist data and a future caller tries to
  pass `None` (or omits the argument) as the docstring says is fine, or
- any new caller is added that relies on the documented default.

## Suggested fix

One-word change in the fallback at `src/linkage/matching.py:308`:

```diff
     if artist_with_wiki_ids_df is None:
         artist_with_wiki_ids_df = pd.DataFrame(
-            columns=[ARTIST_ID_KEY, ARTIST_WIKI_ID_KEY]
+            columns=[ARTIST_ID_KEY, WIKIDATA_ID_KEY]
         )
```

After that fix, `tests/linkage/matching_test.py`'s
`test_default_none_artist_with_wiki_ids_df_raises_instead_of_working` should be
replaced with a test asserting the `None` path actually works (e.g. every
matched wikidata_id gets a freshly minted `artist_id`, not just the cluster's
pre-existing one) — the fixed test would fail loudly to remind whoever applies
this fix.
