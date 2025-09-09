import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
def parse_embedding(val):
        if isinstance(val, str):
            return np.array([float(x) for x in val.strip('[]').split(',')])
        elif isinstance(val, (list, np.ndarray)):
            return np.array(val)
        else:
            return np.nan
def prepare_data_from_parquet(parquet_file_path, id_column_parquet, vector_column_parquet, text_column_parquet):
    data_to_load = None

    print(f"\n--- Setting up LanceDB Table '{table_name}' ---")
    print(f"Attempting to load data from parquet file: {parquet_file_path}")
   
    df = pd.read_parquet(parquet_file_path)
    print(f"Loaded parquet file with shape: {df.shape}")
    # df.head()  # Uncomment to see the first few rows of the DataFrame
    print(df.head())

    # Convert 'embedding' from string to numpy array if needed
    
    if vector_column_parquet in df.columns:
        df[vector_column_parquet] = df[vector_column_parquet].apply(parse_embedding)
        print(f"Converted '{vector_column_parquet}' column to numpy arrays.")
    else:
        print(f"Column '{vector_column_parquet}' not found in DataFrame.")

    # Ensure unique item_id values
    if 'item_id' in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=['item_id'])
        after = len(df)
        print(f"Dropped duplicates in 'item_id': {before - after} rows removed, {after} unique rows remain.")
    else:
        print("Warning: 'item_id' column not found in DataFrame for uniqueness check.")

    # Convert the embedding column to a FixedSizeListArray for LanceDB
    if vector_column_parquet in df.columns:
        arr = np.stack(df[vector_column_parquet].values)
        arr_flat = arr.flatten().tolist()
        arr_value_array = pa.array(arr_flat, pa.float32())
        vector_dim = arr.shape[1]
        arr_fixed = pa.FixedSizeListArray.from_arrays(arr_value_array, vector_dim)
    else:
        print(f"Column '{vector_column_parquet}' not found in DataFrame for FixedSizeListArray conversion.")
        return None

    # Build the pyarrow Table with correct types
    pa_columns = {}
    for col in df.columns:
        if col == vector_column_parquet:
            pa_columns[col] = arr_fixed
        else:
            pa_columns[col] = pa.array(df[col])
    table = pa.table(pa_columns)
    # Validate required columns
    print(table.column_names)  # Debug: Print column names
    required_cols = [id_column_parquet, vector_column_parquet]
    missing_cols = [col for col in required_cols if col not in table.column_names]
    if missing_cols:
        print(f"Error: Missing required columns in parquet file: {missing_cols}")
        return None
    # Rename columns for LanceDB/RAG compatibility
    rename_map = {}
    if id_column_parquet != 'id':
        rename_map[id_column_parquet] = 'id'
    if vector_column_parquet != 'embedding':
        rename_map[vector_column_parquet] = 'embedding'
    if text_column_parquet != 'offer_description' and text_column_parquet in table.column_names:
        rename_map[text_column_parquet] = 'offer_description'
    if rename_map:
        table = table.rename_columns([
            rename_map.get(name, name) for name in table.column_names
        ])
    if 'offer_description' not in table.column_names:
        print("Warning: 'offer_description' column not found in parquet file. RAG features requiring text content might not work as expected.")
    data_to_load = table
    print(f"Loaded {data_to_load.num_rows} records from {parquet_file_path}.")
    print(f"Table schema: {data_to_load.schema}")
    return data_to_load

def setup_lancedb_table(db_path: str, table_name: str,
                        parquet_file_path: str, # No longer optional, as it's the only method
                        id_column_parquet: str = 'item_id',
                        vector_column_parquet: str = 'embedding',
                        text_column_parquet: str = 'offer_description', # Crucial for RAG compatibility from parquet
                        index_type: str = "IVF_PQ", num_partitions: int = 128, num_sub_vectors: int = 96):
    """
    Sets up a LanceDB table directly from a Parquet file.
    If the table already exists, it will be dropped and recreated.

    Args:
        db_path (str): Path to the LanceDB database directory.
        table_name (str): Name of the table to create within LanceDB.
        parquet_file_path (str): Path to the input parquet file containing data.
        id_column_parquet (str): The name of the ID column in the parquet file. Defaults to 'item_id'.
        vector_column_parquet (str): The name of the vector column in the parquet file. Defaults to 'embedding'.
        text_column_parquet (str): The name of the text content column in the parquet file. Defaults to 'offer_description'.
                                   Crucial if RAG queries are intended on parquet data.
        index_type (str): The type of index to create.
        num_partitions (int): Number of partitions for the IVF index.
        num_sub_vectors (int): Number of sub-vectors for the PQ (Product Quantization) index.

    Returns:
        lancedb.table.LanceTable: The created or connected LanceDB table object, or None if an error occurs.
    """
    data_to_load = prepare_data_from_parquet(
    
        parquet_file_path=parquet_file_path,
        id_column_parquet=id_column_parquet,
        vector_column_parquet=vector_column_parquet,
        text_column_parquet=text_column_parquet,
    )

    if data_to_load is None or data_to_load.num_rows == 0:
        print("No data to load into LanceDB table.")
        return None

    db = lancedb.connect(db_path)
    print(f"Connected to LanceDB at: {db_path}")

    # if table_name in db.table_names():
    #     print(f"Table '{table_name}' already exists. Dropping and recreating it.")
    #     db.drop_table(table_name)

    # Create the table
    #Preview data_to_load
    # print(f"data_to_load.head():{data_to_load.head()}")
    table = db.create_table(table_name, data=data_to_load, mode="overwrite")

    print(f"Table '{table_name}' created successfully.")

    # Create a vector index
    print(f"Creating index of type '{index_type}' on column 'embedding'...")
    # FIX: Explicitly specify the column name for indexing
    if 'embedding' not in table.schema.names:
        print(f"Error: 'embedding' column not found in the table schema. Cannot create index.")
        return None # Cannot create index if vector column isn't there

    if index_type == "vector":
        print(f"vector index created with {2} partitions on 'text    '.")
        table.create_index(
        metric="dot",
        num_partitions=8,
        num_sub_vectors=4,
        vector_column_name="embedding",
    )   
        print(f"Index created with {num_partitions} partitions and {num_sub_vectors} sub-vectors.")
    elif index_type == "text":
        print(f"vector text created with {2} partitions on 'text    '.")
        table.create_fts_index("text", replace=True)
    else:
        print(f"Unsupported index type: {index_type}. No index was created.")

    print(f"Indexing complete.")
    return table