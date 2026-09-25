import typer

from cli.deduplicate_artists import main as deduplicate
from cli.embed_offer_names_on_namesakes import main as embed_offer_names
from cli.evaluate import main as evaluate
from cli.link_new_products_to_artists import main as link_new_products
from cli.refresh_artist_metadatas import main as refresh_metadata

app = typer.Typer()

app.command("deduplicate")(deduplicate)
app.command("embed-offer-names")(embed_offer_names)
app.command("link-new-products")(link_new_products)
app.command("refresh-metadata")(refresh_metadata)
app.command("evaluate")(evaluate)

if __name__ == "__main__":
    app()
