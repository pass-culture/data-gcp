import pandas as pd


def get_mapped_fields(legacy_fields_df, new_fields_df):

    mapped_df = legacy_fields_df.merge(new_fields_df, on="name").rename(
        columns={
            "table_id_x": "legacy_table_id",
            "table_id_y": "new_table_id",
            "id_x": "legacy_field_id",
            "id_y": "new_field_id",
        },
    )

    return pd.Series(
        mapped_df.new_field_id.values, index=mapped_df.legacy_field_id
    ).to_dict()


class MetabaseTable:
    def __init__(self, table_name, schema_name, metabase_api):
        self.table_name = table_name
        self.table_schema = schema_name
        self.metabase_api = metabase_api

    def get_table_id(self):

        metabase_tables = self.metabase_api.get_table()
        metabase_tables_df = pd.DataFrame(metabase_tables)[["id", "schema", "name"]]
        table_id = metabase_tables_df.query(
            f'name == "{self.table_name}" and schema == "{self.table_schema}" '
        )["id"].item()
        self.table_id = table_id
        return self.table_id

    def get_table_infos(self):
        self.table_info = self.metabase_api.get_table_metadata(self.table_id)

    def get_fields(self):
        self.get_table_id()
        self.get_table_infos()
        fields = self.table_info["fields"]
        fields_dict = [
            {key: d[key] for key in ["table_id", "name", "id"]} for d in fields
        ]
        fields_df = pd.DataFrame(fields_dict)
        self.fields_df = fields_df
        return self.fields_df
