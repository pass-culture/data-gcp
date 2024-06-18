import typer
from utils.gcs_utils import read_parquet, upload_parquet
from utils.common import setup_matching,multiprocess_matching,link_matches_graph
import pandas as pd
import recordlinkage
app = typer.Typer()

def prepare_tables(indexer,df):
    """
    Preprocess the data.
    """
    table_left = df[['item_id',"batch_id"]].drop_duplicates().reset_index(drop=True)
    table_right = df[['link_item_id',"batch_id"]].drop_duplicates().reset_index(drop=True)
    candidate_links = indexer.index(table_left, table_right)
    df['performer'] = df['performer'].fillna(value='unkn')
    df["offer_name"] = df["offer_name"].str.lower()
    df["offer_description"] = df["offer_description"].str.lower()
    table_left_enriched = pd.merge(table_left, df, on=["item_id"], how="left")
    table_right_enriched = pd.merge(table_right, df, left_on=["link_item_id"], right_on=["item_id"], how="left").drop(columns=["link_item_id"])
    return candidate_links,df,table_left_enriched,table_right_enriched

@app.command()
def main(
    source_gcs_path: str = typer.Option(), output_file_path: str = typer.Option(),
    linkage_candidates_table_path: str = typer.Option(),
    output_table_name: str = typer.Option(),
) -> None:
    indexer = recordlinkage.index.Block(on='batch_id')
    linkage_candidates = pd.read_parquet(f"{source_gcs_path}/{linkage_candidates_table_path}")
    
    candidate_links,catalog_clean,table_left_clean,table_right_clean = prepare_tables(linkage_candidates,indexer)
     
    cpr_cl=setup_matching()
    matches=multiprocess_matching(candidate_links, cpr_cl, table_left_clean, table_right_clean)
    table_left_final,table_right_final=link_matches_graph(matches, table_left_clean, table_right_clean)

    upload_parquet(
        dataframe=table_right_final,
        gcs_path=f"{source_gcs_path}/{output_table_name}_right.parquet",
    )
    upload_parquet(
        dataframe=table_left_final,
        gcs_path=f"{source_gcs_path}/{output_table_name}_left.parquet",
    )

if __name__ == "__main__":
    app()
