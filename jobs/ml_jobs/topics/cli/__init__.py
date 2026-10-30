import typer

from cli.cluster import cluster
from cli.gtl_tree import gtl_tree
from cli.lemmatize import lemmatize
from cli.load_data import load_data
from cli.preprocess import preprocess
from cli.run import run

app = typer.Typer()

for command in (load_data, lemmatize, preprocess, cluster, run, gtl_tree):
    app.command()(command)
