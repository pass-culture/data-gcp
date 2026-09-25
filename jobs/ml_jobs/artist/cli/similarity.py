import typer

from cli.create_similar_artist_parquet import main as create_similar_artist_parquet
from cli.encode_artist_biographies import main as encode_biographies
from cli.get_wikimedia_commons_license import main as get_wikimedia_license
from cli.get_wikipedia_page_content import main as get_wikipedia_content
from cli.summarize_biographies_with_llm import main as summarize_biographies
from cli.transfer_wikimedia_images_to_gcs import main as transfer_images

app = typer.Typer()

app.command("get-wikimedia-license")(get_wikimedia_license)
app.command("transfer-images")(transfer_images)
app.command("get-wikipedia-content")(get_wikipedia_content)
app.command("summarize-biographies")(summarize_biographies)
app.command("encode-biographies")(encode_biographies)
app.command("create-similar-artist-parquet")(create_similar_artist_parquet)

if __name__ == "__main__":
    app()
