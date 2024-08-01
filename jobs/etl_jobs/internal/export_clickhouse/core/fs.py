from jinja2 import Template

BASE_DIR = "schema"


def load_sql(table_name: str, extra_data={}, folder="tmp") -> str:
    with open(f"{BASE_DIR}/{folder}/{table_name}.sql") as file:
        sql_template = file.read()
        return Template(sql_template).render(extra_data)
