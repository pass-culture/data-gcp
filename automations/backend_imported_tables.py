import re
from pathlib import Path

import sqlglot
import sqlglot.expressions as exp
import typer

BASE_PATH = Path(__file__).resolve().parent.parent

PATHS_TO_CHECK = [
    BASE_PATH / "orchestration/dags/dependencies/applicative_database/sql/raw/parallel",
    BASE_PATH
    / "orchestration/dags/dependencies/applicative_database/sql/raw/sequential",
    BASE_PATH / "orchestration/dags/data_gcp_dbt/models/raw/applicative",
]

OUTPUT_FILE = BASE_PATH / "backend_imported_tables.txt"

app = typer.Typer()


def _strip_jinja(sql: str) -> str:
    sql = re.sub(r"\{\{-?\s*config\s*\(.*?\)\s*-?\}}", "", sql, flags=re.S)
    sql = re.sub(
        r'\{\{-?\s*source\s*\(\s*["\'].*?["\']\s*,\s*["\'](.+?)["\']\s*\)\s*-?\}}',
        r"\1",
        sql,
    )
    return sql.strip()


def _parse_sql(sql_content):
    sql = sql_content.replace("\\'", "'")
    sql = _strip_jinja(sql)
    inner_match = re.search(
        r'(?:"""|\'\'\')\s*(SELECT.+?)(?:"""|\'\'\')', sql, flags=re.I | re.S
    )
    if inner_match:
        sql = inner_match.group(1)
    return sqlglot.parse_one(sql, dialect="postgres")


def get_tables_from_sql(sql_content):
    try:
        tree = _parse_sql(sql_content)
        table = tree.find(exp.Table)
        if table:
            return str(f"{table.db}.{table.name}" if table.db else table.name)
        return None
    except sqlglot.ParseError:
        return None


def get_columns_from_sql(sql_content):
    try:
        tree = _parse_sql(sql_content)

        all_tables = list(tree.find_all(exp.Table))
        if not all_tables:
            return []

        first_table = tree.find(exp.Table)
        target_name = first_table.name
        target_db = first_table.db

        candidates = [
            t for t in all_tables if t.name == target_name and t.db == target_db
        ]

        columns = set()
        for table_node in candidates:
            alias = table_node.alias or None
            table_name = table_node.name
            select = table_node.find_ancestor(exp.Select)
            if select is None:
                continue
            for selection in select.expressions:
                for col in selection.find_all(exp.Column):
                    if (
                        not col.table
                        or col.table == alias
                        or (alias is None and col.table == table_name)
                    ):
                        columns.add(col.name)

        return list(columns)
    except sqlglot.ParseError:
        return []


@app.command()
def generate_backend_imported_tables_file():
    imported_tables = {}
    for path in PATHS_TO_CHECK:
        if not path.exists():
            print(f"{path} does not exist")
            continue
        for file_path in path.glob("*.sql"):
            sql_content = file_path.read_text(encoding="utf-8")
            table = get_tables_from_sql(sql_content)
            if table is None:
                print(f"[WARNING] No table found in {file_path.name}, skipped.")
                continue
            columns = get_columns_from_sql(sql_content)
            imported_tables.setdefault(table, set()).update(columns)
    imported_tables = sorted(imported_tables.items())
    lines = [
        f"{table}: {', '.join(sorted(columns))}" for table, columns in imported_tables
    ]
    OUTPUT_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return imported_tables


if __name__ == "__main__":
    app()
