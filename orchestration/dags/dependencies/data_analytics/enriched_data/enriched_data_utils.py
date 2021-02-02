import os

from dependencies.data_analytics.config import BASE32_JS_LIB_PATH

PATH_TO_DIR = os.path.dirname(os.path.realpath(__file__))


def define_humanized_id_query(table, dataset):
    # 1. Define function humanize_id(int) -> str
    humanize_id_definition_query = f"""
        CREATE TEMPORARY FUNCTION humanize_id(id STRING)
        RETURNS STRING
        LANGUAGE js
        OPTIONS (
            library="{BASE32_JS_LIB_PATH}"
          )
        AS \"\"\"
    """

    # open js file and copy code
    with open(os.path.join(PATH_TO_DIR, "humanize_id.js")) as js_file:
        js_code = "\t\t\t".join(js_file.readlines())
    humanize_id_definition_query += "\t\t" + js_code

    humanize_id_definition_query += """
        \"\"\";
    """

    # 2. Use humanize_id function to create (temp) table
    tmp_table_query = f"""
                CREATE TEMP TABLE {table}_humanized_id AS
                    SELECT
                        {table}_id,
                        humanize_id({table}_id) AS humanized_id
                    FROM {dataset}.{table}
                    WHERE {table}_id is not NULL;
    """

    return f"""
        {humanize_id_definition_query}
        {tmp_table_query}
    """
