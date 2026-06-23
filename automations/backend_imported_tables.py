import re
from pathlib import Path

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


def get_tables_from_sql(sql_content):
    return re.findall(r"\bFROM\s+(\w+\.\w+)", sql_content, flags=re.I)


@app.command()
def generate_backend_imported_tables_file():
    imported_tables = []
    for path in PATHS_TO_CHECK:
        if not path.exists():
            print(f"{path} does not exist")
            continue
        for file_path in path.glob("*.sql"):
            sql_content = file_path.read_text(encoding="utf-8")
            table = get_tables_from_sql(sql_content)
            imported_tables.extend(table)
    imported_tables = sorted(imported_tables)
    OUTPUT_FILE.write_text("\n".join(imported_tables), encoding="utf-8")
    return imported_tables


if __name__ == "__main__":
    app()
