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
from handler import NotionDocumentation, NotionGlossary

api_token = access_secret_data(PROJECT_NAME, NOTION_API_KEY_SECRET_NAME)
# notion2md exporter
os.environ["NOTION_TOKEN"] = api_token


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


if __name__ == "__main__":
    typer.run(main)
