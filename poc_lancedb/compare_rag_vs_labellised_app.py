import streamlit as st
import pandas as pd
import re
from poc_chatbot_lancedb import initialize_environment, setup_lancedb_table, rag_query, DB_PATH, TABLE_NAME, DUMMY_PARQUET_FILE_FOR_IMPORT
from tag_descriptions import TAG_DESCRIPTIONS
from loguru import logger
import lancedb
def run():
    st.title("RAG vs Labellised Data Evaluation")

    # File uploader for labellised data
    labellised_file=True
    if labellised_file:
        labellised_df = pd.read_parquet('chatbot_test_dataset_enriched.parquet')
        st.success(f"Loaded {len(labellised_df)} rows from uploaded file.")

        # Initialize environment and LanceDB table
        with st.spinner("Initializing models and LanceDB table..."):
            initialize_environment()
            db = lancedb.connect(DB_PATH)
            if TABLE_NAME in db.table_names():
                lancedb_table = db.open_table(TABLE_NAME)
                st.info(f"Loaded existing LanceDB table: {TABLE_NAME}")
            else:
                lancedb_table = setup_lancedb_table(
                    db_path=DB_PATH,
                    table_name=TABLE_NAME,
                    parquet_file_path=DUMMY_PARQUET_FILE_FOR_IMPORT,
                    id_column_parquet='item_id',
                    vector_column_parquet='embedding',
                    text_column_parquet='offer_description',
                    index_type="vector"
                )
                st.info(f"Created new LanceDB table: {TABLE_NAME}")

        # Function to extract item ids from LLM output
        def extract_ids_from_llm_answer(llm_answer):
            ids=[offer.id for offer in llm_answer.offers]
            return set(ids)

        tag_names = labellised_df['tag_name'].unique().tolist()
        # Map tag_name to a description for the RAG query
        # TAG_DESCRIPTIONS = {
        #     tag: f"Description for {tag}" for tag in tag_names
        # }
        selected_tags = st.multiselect("Select tag_names to evaluate", tag_names, default=tag_names[:3])
        k_retrieval = st.slider("Number of offers to retrieve (k)", 1, 100, 50)

        if st.button("Run RAG Evaluation"):
            results = []
            progress = st.progress(0)
            for i, tag_name in enumerate(selected_tags):
                st.write(f"\n--- Evaluating tag: {tag_name} ---")
                gt_ids = set(labellised_df[labellised_df['tag_name'] == tag_name]['item_id'].astype(str))
                query_text = TAG_DESCRIPTIONS.get(tag_name, tag_name)
                llm_answer = rag_query(lancedb_table, query_text, k_retrieval=k_retrieval)
                logger.info(f"LLM answer for tag '{tag_name}': {llm_answer}")
                pred_ids = extract_ids_from_llm_answer(llm_answer)
                intersection = gt_ids & pred_ids
                precision = len(intersection) / len(pred_ids) if pred_ids else 0
                recall = len(intersection) / len(gt_ids) if gt_ids else 0
                results.append({
                    'tag_name': tag_name,
                    'precision': precision,
                    'recall': recall,
                    'n_gt': len(gt_ids),
                    'n_pred': len(pred_ids),
                    'n_intersection': len(intersection),
                    'ground_truth_ids': list(gt_ids),
                    'predicted_ids': list(pred_ids)
                })
                progress.progress((i+1)/len(selected_tags))
            results_df = pd.DataFrame(results)
            st.success("Evaluation complete!")
            st.dataframe(results_df)
            st.download_button("Download results as CSV", results_df.to_csv(index=False), "rag_vs_labellised_results.csv")
            # Optional: Show details for a selected tag
            tag_to_inspect = st.selectbox("Inspect details for tag_name", selected_tags)
            if tag_to_inspect:
                row = next((r for r in results if r['tag_name'] == tag_to_inspect), None)
                if row:
                    st.write(f"**Ground truth IDs:** {row['ground_truth_ids']}")
                    st.write(f"**Predicted IDs:** {row['predicted_ids']}")
    else:
        st.info("Please upload a labellised parquet file to begin.")
