import typer

from cli import options


def lemmatize(
    *,
    project: str = options.PROJECT,
    ml_input_dataset: str = options.ML_INPUT,
    cache_dir: str = options.CACHE_DIR,
    batch_rows: int = typer.Option(
        100_000, min=1, help="Items read, processed and written at a time"
    ),
    n_jobs: int | None = typer.Option(
        None, min=1, help="Worker processes (default: all cores)"
    ),
    entities: bool = typer.Option(
        False, help="Also extract named entities (JSON, as preprocess does)"
    ),
    entity_types: str = options.ENTITY_TYPES,
    force: bool = typer.Option(
        False, help="Recompute even if the columns already exist"
    ),
) -> None:
    """Add noun_lemmas / verb_lemmas (lowercased lemma lists), and optionally
    entities, for every item of the cached book table, in place."""
    from src.data_loading import cache_path
    from src.lemmatization import add_lemmas

    cache = cache_path(cache_dir, project, ml_input_dataset)
    if not cache.exists():
        raise typer.BadParameter(f"{cache} not found: run load-data first")
    add_lemmas(
        cache,
        entities=entities,
        entity_types=options.parse_entity_types(entity_types),
        batch_rows=batch_rows,
        n_jobs=n_jobs,
        force=force,
    )
