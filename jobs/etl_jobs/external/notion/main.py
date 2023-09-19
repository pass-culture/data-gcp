from utils import (
    PROJECT_NAME,
    access_secret_data,
    NOTION_API_KEY_SECRET_NAME,
    NOTION_DOCUMENTATION_SECRET_NAME,
    NOTION_GLOSSARY_SECRET_NAME,
    ENVIRONMENT_SHORT_NAME,
    RAW_DATASET,
)
import os
import typer
import pandas as pd
import google

from handler import NotionDocumentation, NotionGlossary, BQExport

api_token = access_secret_data(PROJECT_NAME, NOTION_API_KEY_SECRET_NAME)
# notion2md exporter
os.environ["NOTION_TOKEN"] = api_token


def reformat_bq(bq_export: BQExport, doc: list, glossary: list):
    tags = {}
    dataset_id = doc["dataset_id"]
    table_name = doc["table_name"]
    description = doc["description"]
    print(f"updating... {table_name}")
    try:
        source_type = doc["source_type"]
        if len(source_type) > 0:
            tags["source-type"] = ":".join(source_type)
        self_service = doc["self_service"]
        if self_service:
            tags["self-service"] = doc["self_service"].lower()
        owner_team = doc["owner"]
        if owner_team:
            tags["team-owner"] = owner_team.lower()

        columns = {
            glossary["column_name"]: glossary["description"]
            for c in glossary
            if c["table_name"] == table_name.lower()
        }
        bq_export.push_doc(
            dataset_id=f"{dataset_id}_{ENVIRONMENT_SHORT_NAME}",
            table_name=table_name,
            description=description,
            columns=columns,
            labels=tags,
        )
    except google.api_core.exceptions.NotFound:
        print(f"Error with table : {table_name}")


def main():
    glossary_id = access_secret_data(PROJECT_NAME, NOTION_GLOSSARY_SECRET_NAME)
    documentation_id = access_secret_data(
        PROJECT_NAME, NOTION_DOCUMENTATION_SECRET_NAME
    )

    notion_doc = NotionDocumentation(
        database_id=documentation_id, api_key=api_token
    ).export()
    notion_df = pd.DataFrame(notion_doc)
    # convert to str format for export
    notion_df["parents"] = notion_df["parents"].apply(lambda x: ":".join(x))
    notion_df["childrens"] = notion_df["childrens"].apply(lambda x: ":".join(x))
    notion_df["source_type"] = notion_df["source_type"].apply(lambda x: ":".join(x))
    notion_df["parents_name"] = notion_df["parents_name"].apply(lambda x: ":".join(x))
    notion_df["childrens_name"] = notion_df["childrens_name"].apply(
        lambda x: ":".join(x)
    )

    print(notion_df.columns)
    notion_df.to_gbq(f"{RAW_DATASET}.table_documentation", if_exists="replace")

    glossary_doc = NotionGlossary(database_id=glossary_id, api_key=api_token).export()
    glossary_df = pd.DataFrame(glossary_doc)

    glossary_df.to_gbq(f"{RAW_DATASET}.glossary_documentation", if_exists="replace")

    bq_export = BQExport(project_name=PROJECT_NAME)
    for doc in notion_doc:
        reformat_bq(bq_export, doc, glossary_doc)


if __name__ == "__main__":
    typer.run(main)
