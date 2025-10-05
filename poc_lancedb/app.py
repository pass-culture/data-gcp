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
from constants import DB_PATH, TABLE_NAME, SENTENCE_TRANSFORMER_MODEL, OPENAI_LLM_MODEL,GCS_VECTOR_DB_PATH,TABLE_NAME,DB_PATH
from loguru import logger
from tools import download_gcs_database
st.set_page_config(layout="wide")
def main():
    # --- Configuration ---
    
    # --- Load models and LanceDB table ---
    @st.cache_resource(show_spinner=True)
    def load_resources():
        embedding_model, openai_client = initialize_environment()
        if os.path.exists(DB_PATH):
            logger.info(f"LanceDB database found at: {DB_PATH}")
        else:
            download_gcs_database(f"{GCS_VECTOR_DB_PATH}/{TABLE_NAME}.lance")
        db = lancedb.connect(DB_PATH)
        if TABLE_NAME in db.table_names():
            lancedb_table = db.open_table(TABLE_NAME)
            logger.info(f"Loaded existing LanceDB table: {TABLE_NAME}")
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
    "Présentez les offres sélectionnées dans une liste claire et organisée. Pour chaque offre sélectionnée, veuillez inclure :\n"
    "id, pertinence : Expliquez brièvement en français pourquoi cette offre a été retenue pour la thématique sans décrire l'offre ni prendre en compte les autres offres sélectionnées."
    )

    st.subheader("📝 Entrez votre thématique/question et ajustez le prompt si nécessaire")
    col1, col2 = st.columns([1, 3])  # Adjust the ratio as needed
    with col1:
        question = st.text_input("🎯 Votre requête/thématique", "Musique latine")
    with col2:
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
            if llm_answer.offers:
                llm_df = pd.DataFrame([offer.dict() for offer in llm_answer.offers])
                logger.info(f"llm_df length of LLM extracted offers: {len(llm_df)}     ")
                if llm_df.empty:
                    st.warning("⚠️ Aucun résultat trouvé après fusion. Infos de debug :")
                    st.text(f"🆔 IDs extraits : {list(df['id'])}")
                    st.text(f"🆔 IDs du DataFrame :")
                    st.dataframe(df[['id','offer_name','offer_description']], use_container_width=True)
                else:
                    logger.info(f"columns in LLM extracted offers: {llm_df.columns.tolist()}")
                    llm_enriched_df = df.merge(llm_df, left_on='id', right_on='id', how='inner', suffixes=('', '_llm'))
                    df_selected = llm_enriched_df
                    logger.info(f"Columns after merging: {df_selected.columns.tolist()}")
                    logger.info(f"preview of merged DataFrame: {df_selected.head()}  ")
                    # Always keep the full DataFrame ID (with prefix)
                    display_cols = ['id', 'offer_name', 'offer_description','pertinence','offer_subcategory_id' ,'_distance', ]
                    df_selected = df_selected[[col for col in display_cols if col in df_selected.columns]]
                    st.dataframe(df_selected, use_container_width=True)
            else:
                st.warning("⚠️ Problème de parsing coté LLM.")
                st.text(f"llm_answer ():{llm_answer}")
if __name__ == "__main__":
	main()