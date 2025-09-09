import streamlit as st
import numpy as np
import lancedb
import os
import pyarrow as pa
import pandas as pd
from sentence_transformers import SentenceTransformer
from openai import OpenAI
from constants import OPENAI_API_KEY
from poc_chatbot_lancedb import rag_query,initialize_environment
from constants import DB_PATH, TABLE_NAME, SENTENCE_TRANSFORMER_MODEL, OPENAI_LLM_MODEL
from loguru import logger
def run():
    # --- Configuration ---
    
    # --- Load models and LanceDB table ---
    @st.cache_resource(show_spinner=True)
    def load_resources():
        embedding_model, openai_client = initialize_environment()
        db = lancedb.connect(DB_PATH)
        if TABLE_NAME in db.table_names():
            lancedb_table = db.open_table(TABLE_NAME)
            st.info(f"Loaded existing LanceDB table: {TABLE_NAME}")
        else:
            st.error(f"LanceDB table '{TABLE_NAME}' not found in database at '{DB_PATH}'. Please set up the table first.")
            return None, None, None
        return embedding_model, openai_client, lancedb_table

    embedding_model, openai_client, lancedb_table = load_resources()

    # --- Streamlit UI ---
    st.title("RAG Chatbot with LanceDB, SentenceTransformers, and OpenAI")

    if embedding_model is None or openai_client is None or lancedb_table is None:
        st.error("Failed to load models or LanceDB table. Check your setup.")
        st.stop()

    # Prompt template
    prompt_default = (
        "You are a cultural curator and event planner. "
        "Your task is to analyze a list of cultural offerings and select those that align with a specific thematic. "
        "There is no limit on the number of offers you can select, but you should focus on relevance and quality."
        "Present the selected offerings in a clear and organized list. For each selected item, please include: id, relevance: Briefly explain why this offering was selected for the thematic."
    )

    st.subheader("Ask a question about the cultural offers:")
    question = st.text_input("Your thematic/question", "Moyen age")
    prompt_template = st.text_area("Prompt template (edit as needed)", prompt_default, height=200)
    k_retrieval = st.slider("Number of offers to retrieve", 1, 1000, 20)

    if st.button("Run RAG Query"):
        with st.spinner("Running RAG query..."):
            llm_answer, table_results = rag_query(
                lancedb_table,
                embedding_model,
                question,
                k_retrieval=k_retrieval,
                custom_prompt=prompt_template
            )

        #
        # --- Merge with input DataFrame ---
        df = pd.DataFrame(table_results)
        logger.info(f"columns in retrieved results: {df.columns.tolist()}")
        # Build a DataFrame from a list of Pydantic objects (llm_answer.offers)
        logger.info(f"LLM extracted offers: {llm_answer.offers}")
        llm_df = pd.DataFrame([offer.dict() for offer in llm_answer.offers])
        logger.info(f"columns in LLM extracted offers: {llm_df.columns.tolist()}")
        llm_enriched_df = df.merge(llm_df, left_on='id', right_on='id', how='left', suffixes=('', '_llm'))
        df_selected = llm_enriched_df
        logger.info(f"Columns after merging: {df_selected.columns.tolist()}")
        logger.info(f"preview of merged DataFrame: {df_selected.head()}  ")
        # Always keep the full DataFrame ID (with prefix)
        display_cols = ['id', 'offer_name', 'offer_description', '_distance', 'relevance']
        df_selected = df_selected[[col for col in display_cols if col in df_selected.columns]]
        st.markdown("### Selected Offers (merged with input DataFrame):")
        st.dataframe(df_selected)
        if df_selected.empty:
            st.warning("No matches found after merging. Debug info:")
            st.text(f"Extracted IDs: {list(extracted_df['id'])}")
            st.text(f"DataFrame IDs: {list(df['id'])}")
        else:
            st.warning("No 'id' column found in retrieved results for merging.")
