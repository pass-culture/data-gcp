import typer

from cli import options
from src.nlp_preprocessing import preprocess as preprocess_sample
from src.storage import read_table, write_table


def preprocess(
    sample_path: str = typer.Option(
        "data/sample", help="Sampled rows from load-data (Parquet preferred)"
    ),
    out: str = options.ENRICHED_OUT,
    entity_types: str = options.ENTITY_TYPES,
) -> None:
    """Run language detection + NER + noun lemmatization on a sampled table."""
    types = options.parse_entity_types(entity_types)
    write_table(preprocess_sample(read_table(sample_path), entity_types=types), out)
