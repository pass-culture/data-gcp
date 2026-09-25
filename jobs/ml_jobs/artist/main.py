import typer

from cli.extraction import app as extraction_app
from cli.linkage import app as linkage_app
from cli.similarity import app as similarity_app

app = typer.Typer()
app.add_typer(extraction_app, name="extraction")
app.add_typer(linkage_app, name="linkage")
app.add_typer(similarity_app, name="similarity")

if __name__ == "__main__":
    app()
