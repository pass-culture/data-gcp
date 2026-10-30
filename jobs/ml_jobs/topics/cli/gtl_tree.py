from pathlib import Path

import typer

from cli import options


def gtl_tree(
    *,
    project: str = options.PROJECT,
    ml_input_dataset: str = options.ML_INPUT,
    cache_dir: str = options.CACHE_DIR,
    refresh: bool = typer.Option(
        False, help="Re-query BigQuery even if cached GTL counts exist"
    ),
    out: str = typer.Option("data/gtl_tree.html", help="HTML page to write"),
    open_page: bool = typer.Option(
        False, "--open", help="Open the page in the browser"
    ),
) -> None:
    """Write an interactive page (tree + table) of the GTL book hierarchy
    with item counts."""
    import webbrowser

    from src.gtl_tree import load_gtl_counts, write_gtl_tree

    cache = Path(cache_dir) / f"gtl_counts__{project}__{ml_input_dataset}.csv"
    counts = load_gtl_counts(cache, project, ml_input_dataset, refresh=refresh)
    page = Path(out)
    write_gtl_tree(counts, page)
    if open_page:
        webbrowser.open(page.resolve().as_uri())
