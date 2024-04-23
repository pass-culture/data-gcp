import typer
import update as update
import refresh as refresh


def run(
    table_name: str = typer.Option(
        ...,
        help="table_name",
    ),
    dataset_name: str = typer.Option(
        ...,
        help="dataset_name",
    ),
    update_date: str = typer.Option(
        ...,
        help="update_date",
    ),
    mode: str = typer.Option(
        "incremental",
        help="incremental / overwrite",
    ),
    source_gs_path: str = typer.Option(
        None,
        help="source_gs_path",
    ),
):
    if mode in ("incremental", "overwrite"):
        if source_gs_path is None:
            raise Exception("source_gs_path should be specified")
        update.main_update(mode, source_gs_path, table_name, dataset_name, update_date)
    else:
        raise Exception(f"wrong specified mode, got {mode} ")


if __name__ == "__main__":
    typer.run(run)
