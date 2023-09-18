from utils import (
    PROJECT_NAME,
    access_secret_data,
    NOTION_API_KEY_SECRET_NAME,
    NOTION_DOCUMENTATION_SECRET_NAME,
    NOTION_GLOSSARY_SECRET_NAME,
    RAW_DATASET,
)
import os
import typer
import pandas as pd
from handler import NotionDocumentation, NotionGlossary, BQExport

api_token = access_secret_data(PROJECT_NAME, NOTION_API_KEY_SECRET_NAME)
# notion2md exporter
os.environ["NOTION_TOKEN"] = api_token


def reformat_bq(bq_export: BQExport, doc: list, glossary: list):
    tags = {}
    dataset_id = doc["dataset_id"]
    table_name = doc["table_name"]
    description = doc["description"]

    source_type = doc["source_type"]
    if len(source_type) > 0:
        tags["source_type"] = ":".join(source_type)
    self_service = doc["self_service"]
    if self_service:
        tags["self_service"] = doc["self_service"]
    owner_team = doc["owner"]
    if owner_team:
        tags["owner"] = owner_team

    columns = {
        glossary["column_name"]: glossary["description"]
        for c in glossary
        if c["table_name"] == table_name
    }
    bq_export.push_doc(
        dataset_id=dataset_id,
        table_name=table_name,
        description=description,
        columns=columns,
        labels=tags,
    )


def main():
    glossary_id = access_secret_data(PROJECT_NAME, NOTION_GLOSSARY_SECRET_NAME)
    documentation_id = access_secret_data(
        PROJECT_NAME, NOTION_DOCUMENTATION_SECRET_NAME
    )

    notion_doc = NotionDocumentation(
        database_id=documentation_id, api_key=api_token
    ).export()
    pd.DataFrame(notion_doc).to_gbq(f"{RAW_DATASET}.table_documentation")

    glossary_doc = NotionGlossary(database_id=glossary_id, api_key=api_token).export()
    pd.DataFrame(glossary_doc).to_gbq(f"{RAW_DATASET}.glossary_documentation")

    bq_export = BQExport(project_name=PROJECT_NAME)
    for doc in notion_doc[:1]:
        reformat_bq(bq_export, doc, glossary_doc)


if __name__ == "__main__":
    typer.run(main)
