from cli import options
from src.data_loading import load_and_sample
from src.storage import write_table


def load_data(
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
) -> None:
    """Load book items from BigQuery (or cache) and draw a volume-unbiased sample."""
    sample = load_and_sample(
        cache_dir,
        project,
        sample_size,
        strata_levels,
        seed,
        ml_input_dataset,
        lang=None if lang == "all" else lang,
        refresh=refresh,
    )
    write_table(sample, sample_out)
