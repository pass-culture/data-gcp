import pandas as pd
import numpy as np
import lancedb
import os
from loguru import logger
import pyarrow as pa
from openai import OpenAI # For OpenAI LLM
from sentence_transformers import SentenceTransformer # For SentenceTransformer embeddings


from constants import OPENAI_API_KEY,OPENAI_LLM_MODEL,PARQUET_FILE, DB_PATH, TABLE_NAME,SENTENCE_TRANSFORMER_MODEL,DUMMY_PARQUET
from llm_tools import build_prompt,llm_agent
from tools import create_dummy_table
from vector_database import setup_lancedb_table

def initialize_environment():
    """
    Initializes the script's environment by:
    1. Loading the SentenceTransformer model for text embedding.
    2. Setting up the OpenAI client.
    3. Checking for the presence of the OPENAI_API_KEY environment variable.
    4. Uses a real Parquet file if it exists, otherwise generates dummy data.
    """

    # Setup logging

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
    if not OPENAI_API_KEY:
        logger.warning("OPENAI_API_KEY environment variable not set. OpenAI LLM features will not work.")
        logger.warning("To enable OpenAI LLM, please set the OPENAI_API_KEY environment variable.")
    else:
        try:
            openai_client = OpenAI(api_key=OPENAI_API_KEY)
            logger.info(f"OpenAI client initialized for {OPENAI_LLM_MODEL} LLM.")
        except Exception as e:
            logger.error(f"Error initializing OpenAI client: {e}")
            logger.error("Please ensure your OPENAI_API_KEY is correct and you have the latest `openai` library installed (`pip install --upgrade openai`).")
            openai_client = None
    if os.path.exists(PARQUET_FILE):
        logger.info(f"Using existing Parquet file: {PARQUET_FILE}")
    else:
        logger.warning(f"Parquet file '{PARQUET_FILE}' not found. Generating dummy data.")
        create_dummy_table(DUMMY_PARQUET)
    return embedding_model, openai_client

# --- LanceDB Table Setup (For Parquet Import) ---
def rag_query(table, embedding_model, question: str, k_retrieval: int = 5, custom_prompt: str = None):
    if embedding_model is None:
        return "SentenceTransformer embedding model not loaded. Cannot perform RAG query."
    if 'offer_description' not in table.schema.names:
        return "Error: RAG query requires 'offer_description' column in the LanceDB table, but it's missing."

    print(f"\n--- Performing RAG Query for: '{question}' ---")
    question_vector = embedding_model.encode(question).tolist()
    table_results = table.search(question_vector).limit(k_retrieval).to_list()
    if not table_results:
        return "No relevant documents found in the database for the given question."
    prompt = build_prompt(question, table_results,custom_prompt)
    logger.info("### Prompt sent to LLM:")
    logger.info(prompt)
    llm_result = llm_agent.run_sync(prompt)
    logger.info("### Raw LLM output:")
    logger.info(llm_result.output)
    return llm_result.output,table_results

# --- Main Execution Block ---
if __name__ == "__main__":
    # 1. Initialize the environment: This step loads both models and checks API keys.
    embedding_model, openai_client = initialize_environment()

    # Ensure necessary components are loaded
    if embedding_model is None:
        print("\nSkipping further execution: SentenceTransformer model could not be loaded.")
        exit()
    # It's okay if openai_client is None here; RAG query itself will check and inform if LLM part cannot run.

    # This option allows you to import an existing parquet file.
    # Make sure your parquet file contains an ID column, a vector column, and optionally a text content column.
    print("\n*** Running with Parquet file data import (Hybrid: ST Embeddings, OpenAI LLM) ***")
    # Check if LanceDB table already exists in DB_PATH
    db = lancedb.connect(DB_PATH)
    if TABLE_NAME in db.table_names():
        print(f"\nTable '{TABLE_NAME}' already exists in LanceDB at '{DB_PATH}'. Using existing table.")
        lancedb_table = db.open_table(TABLE_NAME)
    else:
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
        user_question = "Moyen age"

        llm_answer, table_results = rag_query(
            lancedb_table,
            embedding_model,
            user_question,
            k_retrieval=50
        )
    else:
        print("Skipping RAG Query for Parquet data: 'offer_description' column missing or OpenAI LLM not available.")

    print("\nScript execution finished.")
    print(f"\nTo clean up, you can delete the database directory '{DB_PATH}' and '{DUMMY_PARQUET_FILE_FOR_IMPORT}' file.")
