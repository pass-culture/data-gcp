from jinja2 import Template

BASE_DIR = "schema"


def load_sql(
    table_name: str, extra_data: dict | None = None, folder: str = "tmp"
) -> str:
    if extra_data is None:
        extra_data = {}
    path = f"{BASE_DIR}/{folder}/{table_name}.sql"
    try:
        with open(path) as file:
            sql_template = file.read()
            return Template(sql_template).render(extra_data)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"SQL schema file not found: {path}") from e
