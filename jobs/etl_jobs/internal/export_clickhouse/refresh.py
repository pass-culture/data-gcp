import typer
from utils import refresh_views


def run(
    view_name: str = typer.Option(
        ...,
        help="view_name",
    ),
):
    refresh_views(view_name)


if __name__ == "__main__":
    typer.run(run)
