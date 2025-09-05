import streamlit as st
import numpy as np
import lancedb
import os
import pyarrow as pa
import pandas as pd
from sentence_transformers import SentenceTransformer
from openai import OpenAI

def run():
    # --- Configuration ---
    DB_PATH = "lancedb_parquet_rag.db"
    TABLE_NAME = "my_rag_data"
    PARQUET_FILE = "item_embedding_chatbot.parquet"
    SENTENCE_TRANSFORMER_MODEL = 'all-MiniLM-L6-v2'
    OPENAI_LLM_MODEL = "gpt-3.5-turbo"

    # --- Load models and LanceDB table ---
    @st.cache_resource(show_spinner=True)
    def load_resources():
        embedding_model = SentenceTransformer(SENTENCE_TRANSFORMER_MODEL)
        openai_api_key = os.getenv("OPENAI_API_KEY", "")
        openai_client = OpenAI(api_key=openai_api_key) if openai_api_key else None
        db = lancedb.connect(DB_PATH)
        table = db.open_table(TABLE_NAME)
        return embedding_model, openai_client, table

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
        "\n\nContext:\n{context}\n\nAnalyze the provided cultural offerings and identify all events, exhibitions, or performances that fit the following thematic: \"{question}\"\n\n"
        "Present the selected offerings in a clear and organized list. For each selected item, please include: id, relevance: Briefly explain why this offering was selected for the thematic.\n"
    )

    st.subheader("Ask a question about the cultural offers:")
    question = st.text_input("Your thematic/question", "Moyen age")
    prompt_template = st.text_area("Prompt template (edit as needed)", prompt_default, height=200)
    k_retrieval = st.slider("Number of offers to retrieve", 1, 1000, 20)

    if st.button("Run RAG Query"):
        with st.spinner("Running RAG query..."):
            # Embed the question
            question_vector = embedding_model.encode(question).tolist()
            # Retrieve from LanceDB
            retrieved_results = lancedb_table.search(question_vector).limit(k_retrieval).to_list()
            if not retrieved_results:
                st.warning("No relevant documents found in the database for the given question.")
                st.stop()
            # Build context string
            context = "\n".join([
                f"{res.get('id', '')}: {res.get('offer_name', '')}, {res.get('offer_description', '')}"
                for res in retrieved_results if res.get('offer_name')
            ])
            # Build prompt
            prompt = prompt_template.format(context=context, question=question)
            messages = [
                {"role": "system", "content": prompt}
            ]
            # Call OpenAI
            try:
                response = openai_client.chat.completions.create(
                    model=OPENAI_LLM_MODEL,
                    messages=messages,
                    temperature=0.7,
                    max_tokens=2000
                )
                llm_answer = response.choices[0].message.content if response.choices and response.choices[0].message else "No answer returned."
                st.markdown("### LLM Answer:")
                st.write(llm_answer)

                # --- Extract IDs and relevance from LLM output ---
                import re
                # Will store tuples: (id, relevance)
                extracted = []
                for line in llm_answer.splitlines():
                    # Try to match: id: product-123 ... relevance: ...
                    id_match = re.search(r"id\s*[:=\-]\s*([\w\-]+)", line, re.IGNORECASE)
                    if not id_match:
                        # Try to match if ID is at the start of the line, e.g. 'product-123:'
                        id_match = re.match(r"\s*([\w\-]+)\s*[:=\-]", line)
                    if id_match:
                        id_val = id_match.group(1)
                        # Try to extract relevance after 'relevance:' or 'relevance -' etc.
                        rel_match = re.search(r"Relevance\s*[:=\-]\s*(.*)", line, re.IGNORECASE)
                        relevance = rel_match.group(1).strip() if rel_match else ''
                        extracted.append((id_val, relevance))
                if not extracted:
                    # Fallback: find all product-xxx or offer-xxx like patterns
                    ids = set(re.findall(r"\b(?:product|offer)-[\w\d]+\b", llm_answer, re.IGNORECASE))
                    extracted = [(id_val, '') for id_val in ids]
                st.info(f"Extracted {len(extracted)} ids from LLM output.")

                # --- Merge with input DataFrame ---
                df = pd.DataFrame(retrieved_results)
                if 'id' in df.columns:
                    # Extract numeric part from DataFrame IDs
                    df['id_numeric'] = df['id'].astype(str).str.extract(r'(\d+)$')[0]
                    extracted_df = pd.DataFrame(extracted, columns=['id', 'relevance'])
                    extracted_df['id_numeric'] = extracted_df['id'].astype(str).str.extract(r'(\d+)$')[0]
                    # Merge on numeric part
                    df_selected = df.merge(extracted_df, on='id_numeric', how='inner', suffixes=('', '_llm'))
                    # Always keep the full DataFrame ID (with prefix)
                    df_selected['id'] = df_selected['id']  # This is the full ID from the DataFrame
                    # Filter columns
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

            except Exception as e:
                st.error(f"OpenAI error: {e}")
            # Optionally show context
            with st.expander("Show RAG context"):
                st.text(context)
