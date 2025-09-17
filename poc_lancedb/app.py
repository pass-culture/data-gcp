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
def main():
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
    st.title("🤖 Prototype de chatbot éditorial")

    if embedding_model is None or openai_client is None or lancedb_table is None:
        st.error("Failed to load models or LanceDB table. Check your setup.")
        st.stop()

    # Prompt template
    prompt_default = (
    "Vous êtes un(e) curateur(trice) culturel(le) et organisateur(trice) d'événements. \n"
    "Votre tâche est d'analyser une liste d'offres culturelles et de sélectionner celles qui correspondent à une thématique spécifique. \n"
    "Il n'y a pas de limite au nombre d'offres que vous pouvez sélectionner, mais vous devez privilégier la pertinence et la qualité.\n"
    "Présentez les offres sélectionnées dans une liste claire et organisée. Pour chaque offre sélectionnée, veuillez inclure : id, pertinence : Expliquez brièvement en français pourquoi cette offre a été retenue pour la thématique.\n"
    )

    st.subheader("📝 Entrez votre thématique/question et ajustez le prompt si nécessaire")
    question = st.text_input("🎯 Votre requête/thématique", "Moyen age")
    prompt_template = st.text_area("🛠️ Template de prompt (éditez si besoin)", prompt_default, height=200)
    k_retrieval = 100

    if st.button("🚀 Exécuter la requête"):
        with st.spinner("⏳ Recherche en cours..."):
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
        logger.info(f"df length of retrieved results: {len(df)}     ")
        # logger.info(f"llm_answer len: {len(llm_answer.offers)}  ")
        logger.info(f"df preview of retrieved results: {df.head()}  ")
        st.markdown("<h3>🧠 Offres extraites par le LLM :</h3>", unsafe_allow_html=True)
        # st.write(llm_answer)
        output_dataframe=True
        if output_dataframe:
            llm_df = pd.DataFrame([offer.dict() for offer in llm_answer.offers])
            logger.info(f"columns in LLM extracted offers: {llm_df.columns.tolist()}")
            llm_enriched_df = df.merge(llm_df, left_on='id', right_on='id', how='inner', suffixes=('', '_llm'))
            df_selected = llm_enriched_df
            logger.info(f"Columns after merging: {df_selected.columns.tolist()}")
            logger.info(f"preview of merged DataFrame: {df_selected.head()}  ")
            # Always keep the full DataFrame ID (with prefix)
            display_cols = ['id', 'offer_name', 'offer_description','offer_subcategory_id' ,'_distance', 'relevance']
            df_selected = df_selected[[col for col in display_cols if col in df_selected.columns]]
            st.markdown("<h3>🎉 Offres sélectionnées (fusionnées avec les données d'entrée) :</h3>", unsafe_allow_html=True)
            st.dataframe(df_selected, use_container_width=True)
            if df_selected.empty:
                st.warning("⚠️ Aucun résultat trouvé après fusion. Infos de debug :")
                st.text(f"🆔 IDs extraits : {list(df_selected['id'])}")
                st.text(f"🆔 IDs du DataFrame : {list(df['id'])}")
if __name__ == "__main__":
	main()