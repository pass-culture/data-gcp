from cli import options
from cli.load_data import load_data
from cli.preprocess import preprocess


def run(
    *,
    sample_size: int = options.SAMPLE_SIZE,
    strata_levels: int = options.STRATA_LEVELS,
    seed: int = options.SAMPLE_SEED,
    project: str = options.PROJECT,
    ml_input_dataset: str = options.ML_INPUT,
    cache_dir: str = options.CACHE_DIR,
    lang: str = options.LANG,
    refresh: bool = options.REFRESH,
    sample_out: str = options.SAMPLE_OUT,
    out: str = options.ENRICHED_OUT,
    entity_types: str = options.ENTITY_TYPES,
) -> None:
    """load-data followed by preprocess, end to end."""
    load_data(
        sample_size=sample_size,
        strata_levels=strata_levels,
        seed=seed,
        project=project,
        ml_input_dataset=ml_input_dataset,
        cache_dir=cache_dir,
        lang=lang,
        refresh=refresh,
        sample_out=sample_out,
    )
    preprocess(sample_out, out, entity_types)
