import pandas as pd
import numpy as np
import lancedb
import os
import logging
import pyarrow as pa
from openai import OpenAI # For OpenAI LLM
from sentence_transformers import SentenceTransformer # For SentenceTransformer embeddings
from pydantic import BaseModel
from pydantic_ai import Agent
from typing import List

# --- Configuration ---
# Path where LanceDB will store its data. This will create a directory.
DB_PATH = "lancedb_parquet_rag.db" # Adjusted DB path for parquet-only setup
# Name of the table within the LanceDB database
TABLE_NAME = "my_rag_data" # Adjusted table name for parquet-only setup
# Dummy parquet file path for demonstration purposes when importing from parquet.
DUMMY_PARQUET_FILE_FOR_IMPORT = "chatbot_test_dataset_enriched.parquet" # Keeping existing dummy file name

# Global variables for models and API keys
embedding_model = None # For SentenceTransformer
openai_api_key = None
openai_client = None
SENTENCE_TRANSFORMER_MODEL = 'all-MiniLM-L6-v2' # Model for embeddings
OPENAI_LLM_MODEL = "gpt-3.5-turbo" # Or "gpt-4" for higher quality

def initialize_environment():
    """
    Initializes the script's environment by:
    1. Loading the SentenceTransformer model for text embedding.
    2. Setting up the OpenAI client.
    3. Checking for the presence of the OPENAI_API_KEY environment variable.
    4. Uses a real Parquet file if it exists, otherwise generates dummy data.
    """
    global embedding_model, openai_api_key, openai_client, DUMMY_PARQUET_FILE_FOR_IMPORT

    # Setup logging
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
    logger = logging.getLogger("chatbot_lancedb")

    logger.info("--- Initializing Environment ---")

    # 1. Initialize the SentenceTransformer embedding model
    try:
        embedding_model = SentenceTransformer(SENTENCE_TRANSFORMER_MODEL)
        logger.info(f"SentenceTransformer model '{SENTENCE_TRANSFORMER_MODEL}' loaded successfully.")
    except Exception as e:
        logger.error(f"Error loading SentenceTransformer model: {e}")
        logger.error("Please ensure 'sentence-transformers' is installed: pip install sentence-transformers")
        embedding_model = None # Set to None if loading fails

    # 2. Get OpenAI API key from environment variable and set it for openai
    openai_api_key = os.getenv("OPENAI_API_KEY", "")
    if not openai_api_key:
        logger.warning("OPENAI_API_KEY environment variable not set. OpenAI LLM features will not work.")
        logger.warning("To enable OpenAI LLM, please set the OPENAI_API_KEY environment variable.")
    else:
        try:
            openai_client = OpenAI(api_key=openai_api_key)
            logger.info(f"OpenAI client initialized for {OPENAI_LLM_MODEL} LLM.")
        except Exception as e:
            logger.error(f"Error initializing OpenAI client: {e}")
            logger.error("Please ensure your OPENAI_API_KEY is correct and you have the latest `openai` library installed (`pip install --upgrade openai`).")
            openai_client = None

    # 3. Use real Parquet file if it exists, otherwise generate dummy data
    if os.path.exists(DUMMY_PARQUET_FILE_FOR_IMPORT):
        logger.info(f"Using existing Parquet file: {DUMMY_PARQUET_FILE_FOR_IMPORT}")
    else:
        logger.warning(f"Parquet file '{DUMMY_PARQUET_FILE_FOR_IMPORT}' not found. Generating dummy data.")
        data_size = 500  # Number of dummy records
        vector_dim = 384  # Dimension for SentenceTransformer's all-MiniLM-L6-v2

        ids = [f"item_{i:03d}" for i in range(data_size)]
        vectors = np.random.rand(data_size, vector_dim).astype(np.float32)
        texts = [
            f"This is a dummy text description for item {i}. It covers topics like data science, AI, and machine learning. This is more text to make it longer and more diverse."
            for i in range(data_size)
        ]

        # Use PyArrow to create a FixedSizeListArray for the vector column
        import pyarrow as pa
        ids_array = pa.array(ids, pa.string())
        texts_array = pa.array(texts, pa.string())
        # Convert the numpy array to a flat list, then to a FixedSizeListArray
        vector_flat = vectors.flatten().tolist()
        vector_value_array = pa.array(vector_flat, pa.float32())
        vector_list_array = pa.FixedSizeListArray.from_arrays(vector_value_array, vector_dim)

        table = pa.table({
            'item_id': ids_array,
            'embedding': vector_list_array,
            'offer_description': texts_array
        })
        # Write to Parquet
        import pyarrow.parquet as pq
        pq.write_table(table, DUMMY_PARQUET_FILE_FOR_IMPORT)
        logger.info(f"Generated dummy parquet file for direct import: {DUMMY_PARQUET_FILE_FOR_IMPORT} with {data_size} records.")


# --- LanceDB Table Setup (For Parquet Import) ---
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
    data_to_load = None


    print(f"\n--- Setting up LanceDB Table '{table_name}' ---")
    print(f"Attempting to load data from parquet file: {parquet_file_path}")
    import pyarrow as pa
    import pyarrow.parquet as pq

    try:
        # Load as DataFrame to allow conversion if needed
        import pandas as pd
        df = pd.read_parquet(parquet_file_path)
        print(f"Loaded parquet file with shape: {df.shape}")
        # df.head()  # Uncomment to see the first few rows of the DataFrame
        print(df.head())

        # Convert 'embedding' from string to numpy array if needed
        def parse_embedding(val):
            if isinstance(val, str):
                return np.array([float(x) for x in val.strip('[]').split(',')])
            elif isinstance(val, (list, np.ndarray)):
                return np.array(val)
            else:
                return np.nan
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
    except Exception as e:
        print(f"Error reading parquet file {parquet_file_path}: {e}")
        return None

    if data_to_load is None or data_to_load.num_rows == 0:
        print("No data to load into LanceDB table.")
        return None

    try:
        db = lancedb.connect(db_path)
        print(f"Connected to LanceDB at: {db_path}")

        if table_name in db.table_names():
            print(f"Table '{table_name}' already exists. Dropping and recreating it.")
            db.drop_table(table_name)

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
    except Exception as e:
        print(f"Error setting up LanceDB table: {e}")
        return None

# --- RAG Query Function ---
class OfferSelection(BaseModel):
    id: str
    relevance: str

class LLMOutput(BaseModel):
    offers: List[OfferSelection]

# Create the agent for LLM calls and parsing
llm_agent = Agent(
    'openai:gpt-3.5-turbo',  # or 'openai:gpt-4o-mini' if available
    output_type=[LLMOutput, str],
    system_prompt=(
        "Extract all selected offers from the context. "
        "For each offer, provide: id (product-ID or offer-ID), relevance (brief explanation). "
        "If you can't extract all data, ask the user to try again. "
        "Return the result as a JSON object with an 'offers' list."
    ),
)

def rag_query(table, question: str, k_retrieval: int = 5):
    global embedding_model
    if embedding_model is None:
        return "SentenceTransformer embedding model not loaded. Cannot perform RAG query."
    if 'offer_description' not in table.schema.names:
        return "Error: RAG query requires 'offer_description' column in the LanceDB table, but it's missing."

    print(f"\n--- Performing RAG Query for: '{question}' ---")
    try:
        question_vector = embedding_model.encode(question).tolist()
        retrieved_results = table.search(question_vector).limit(k_retrieval).to_list()
        if not retrieved_results:
            return "No relevant documents found in the database for the given question."
        context = "\n".join([
            f"{res.get('id', '')}: {res.get('offer_name', '')}, {res.get('offer_description', '')}"
            for res in retrieved_results if res.get('offer_name')
        ])
        prompt = (
            "You are a cultural curator and event planner. "
            "Your task is to analyze a list of cultural offerings and select those that align with a specific thematic. "
            "There is no limit on the number of offers you can select, but you should focus on relevance and quality. "
            f"\n\nContext:\n{context}\n\nAnalyze the provided cultural offerings and identify all events, exhibitions, or performances that fit the following thematic: '{question}'\n\n"
            "Present the selected offerings in a clear and organized list. For each selected item, please include: id, relevance. Return the result as a JSON object with an 'offers' list."
        )
        result = llm_agent.run_sync(prompt)
        return result.output
    except Exception as e:
        print(f"Error during RAG query with pydantic-ai Agent: {e}")
        return f"RAG query failed due to an LLM error: {e}"

# --- Main Execution Block ---
if __name__ == "__main__":
    # 1. Initialize the environment: This step loads both models and checks API keys.
    initialize_environment()

    # Ensure necessary components are loaded
    if embedding_model is None:
        print("\nSkipping further execution: SentenceTransformer model could not be loaded.")
        exit()
    # It's okay if openai_client is None here; RAG query itself will check and inform if LLM part cannot run.

    # This option allows you to import an existing parquet file.
    # Make sure your parquet file contains an ID column, a vector column, and optionally a text content column.
    print("\n*** Running with Parquet file data import (Hybrid: ST Embeddings, OpenAI LLM) ***")
    lancedb_table = setup_lancedb_table(
        db_path=DB_PATH,
        table_name=TABLE_NAME,
        parquet_file_path=DUMMY_PARQUET_FILE_FOR_IMPORT, # Use the dummy file created at init
        id_column_parquet='item_id', # Name of the ID column in your parquet file
        vector_column_parquet='embedding', # Name of the vector column in your parquet file
        text_column_parquet='offer_description', # Name of the text content column in your parquet file (for RAG compatibility)
        index_type="vector" # Choose "IVF_PQ" or "IVFFlat"
    )

    if lancedb_table is None:
        print("\nFailed to set up LanceDB table with Parquet data. Exiting script.")
        exit()

    # If you loaded data from parquet and it has 'offer_description' and OpenAI API key is available, you can run RAG queries.
    if 'offer_description' in lancedb_table.schema.names and openai_client:
        print("\n--- Demonstrating RAG Query on Parquet Imported Data (Hybrid: ST Embeddings, OpenAI LLM) ---")
        user_question_parquet = "Moyen age"
        
        rag_answer_parquet = rag_query(
            lancedb_table,
            user_question_parquet,
            k_retrieval=50
        )
    else:
        print("Skipping RAG Query for Parquet data: 'offer_description' column missing or OpenAI LLM not available.")

    print("\nScript execution finished.")
    print(f"\nTo clean up, you can delete the database directory '{DB_PATH}' and '{DUMMY_PARQUET_FILE_FOR_IMPORT}' file.")
