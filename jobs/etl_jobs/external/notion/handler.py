from notion_client import Client
from notion2md.exporter.block import StringExporter
from google.cloud import bigquery
from utils import ENVIRONMENT_SHORT_NAME


class BQExport:
    def __init__(self, project_name):
        self.project_name = project_name
        self.client = bigquery.Client()

    def push_doc(
        self,
        dataset_id: str,
        table_name: str,
        description: str,
        columns: dict,
        labels: dict,
    ):
        dataset_ref = bigquery.DatasetReference(self.project_name, dataset_id)
        table_ref = dataset_ref.table(table_name)
        table = self.client.get_table(table_ref)
        export_schema = []
        for s in table.schema:
            description = columns.get(s.name, None)
            if description is not None:
                s = bigquery.SchemaField(
                    name=s.name,
                    field_type=s.field_type,
                    mode=s.mode,
                    default_value_expression=s.default_value_expression,
                    description=description,
                )

            export_schema.append(s)
        table.schema = export_schema
        table.description = description
        table.labels = labels
        table = self.client.update_table(table, ["description", "labels", "schema"])


class NotionGlossary:
    def __init__(self, database_id, api_key):
        self.database_id = database_id
        self.notion = Client(auth=api_key)

    @staticmethod
    def get_description(document):
        try:
            return document["properties"]["Définition"]["rich_text"][0]["plain_text"]
        except IndexError:
            return None

    @staticmethod
    def get_column(document):
        try:
            return document["properties"]["Nom"]["title"][0]["plain_text"]
        except IndexError:
            return None

    @staticmethod
    def get_table(document):
        try:
            return document["properties"]["Tables ou métrique"]["multi_select"]
        except IndexError:
            return []

    def export(self):
        has_more = True
        extra_args = {}

        while has_more:
            my_page = self.notion.databases.query(
                **dict({"database_id": self.database_id}, **extra_args)
            )
            has_more = my_page["has_more"]
            extra_args = {"start_cursor": my_page.get("next_cursor", None)}
            glossary_arr = []
            for document in my_page["results"]:
                for table_desc in self.get_table(document):
                    table_name = table_desc["name"]
                    export_dict = {
                        "id": document["id"],
                        "url": document["url"],
                        "column_name": self.get_column(document),
                        "description": self.get_description(document),
                        "table_name": table_name,
                        "created_at": document["created_time"],
                        "created_by": document["created_by"]["id"],
                        "edited_at": document["last_edited_time"],
                        "edited_by": document["last_edited_by"]["id"],
                    }
                    glossary_arr.append(export_dict)

        return glossary_arr


class NotionDocumentation(NotionGlossary):
    @staticmethod
    def get_source_type(document):
        try:
            return [
                x["name"]
                for x in document["properties"]["Type de Source"]["multi_select"]
            ]
        except IndexError:
            return []

    @staticmethod
    def get_parents(document):
        try:
            return [x["id"] for x in document["properties"]["Dépendances"]["relation"]]
        except IndexError:
            return []

    @staticmethod
    def get_childrens(document):
        try:
            return [x["id"] for x in document["properties"]["Lié à Tables"]["relation"]]
        except IndexError:
            return []

    @staticmethod
    def get_dataset(document):
        try:
            return document["properties"]["Dataset"]["select"]["name"]
        except TypeError:
            return None

    @staticmethod
    def get_self_service(document):
        try:
            return document["properties"]["Self-service"]["select"]["name"]
        except TypeError:
            return None

    @staticmethod
    def get_team_owner(document):
        try:
            return document["properties"]["Owner"]["select"]["name"]
        except TypeError:
            return None

    @staticmethod
    def get_simple_description(document):
        try:
            return document["properties"]["Description"]["title"][0]["plain_text"]
        except (TypeError, KeyError):
            return None

    @staticmethod
    def get_table_name(document):
        try:
            return document["properties"]["Nom"]["title"][0]["plain_text"]
        except (TypeError, IndexError):
            return None

    def export(self):
        has_more = True
        extra_args = {}

        while has_more:
            my_page = self.notion.databases.query(
                **dict({"database_id": self.database_id}, **extra_args)
            )
            has_more = my_page["has_more"]
            extra_args = {"start_cursor": my_page.get("next_cursor", None)}
            table_arr = []

            for document in my_page["results"]:
                page_id = document["id"]
                export_dict = {
                    "id": page_id,
                    "url": document["url"],
                    "created_at": document["created_time"],
                    "created_by": document["created_by"]["id"],
                    "edited_at": document["last_edited_time"],
                    "edited_by": document["last_edited_by"]["id"],
                    "dataset_id": self.get_dataset(document),
                    "table_name": self.get_table_name(document),
                    "source_type": self.get_source_type(document),
                    "parents": self.get_parents(document),
                    "childrens": self.get_childrens(document),
                    "self_service": self.get_self_service(document),
                    "owner": self.get_team_owner(document),
                    "description": self.get_simple_description(document),
                    "full_description": StringExporter(block_id=page_id).export(),
                }
                table_arr.append(export_dict)

        id_name = {x["id"]: x["table_name"] for x in table_arr}
        for table in table_arr:
            parents_table_name = []
            for parent in table["parents"]:
                parents_table_name.append(id_name[parent])
            table["parents_name"] = parents_table_name

            childrens_table_name = []
            for children in table["childrens"]:
                childrens_table_name.append(id_name[children])
            table["childrens_name"] = childrens_table_name

        return table_arr
