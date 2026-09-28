import typer

app = typer.Typer()


@app.command()
def hello() -> None:
    """Replace with your first command."""
    print("Hello, world!")
