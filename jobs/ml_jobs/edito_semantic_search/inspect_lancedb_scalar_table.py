import os
import lancedb
import pandas as pd
from constants import DATABASE_URI, SCALAR_TABLE

def main():
    print(f"Connecting to LanceDB at: {DATABASE_URI}")
    db = lancedb.connect(DATABASE_URI)
    print(f"Opening table: {SCALAR_TABLE}")
    table = db.open_table(SCALAR_TABLE)
    print("\nTable columns:")
    print(table.schema)
    print("\nSample rows:")
    df = table.search().limit(10).to_pandas()
    print(df.head(10))

if __name__ == "__main__":
    main()
