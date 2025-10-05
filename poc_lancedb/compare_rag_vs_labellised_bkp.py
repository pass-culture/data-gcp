import pandas as pd
import re
from poc_chatbot_lancedb import initialize_environment, setup_lancedb_table, rag_query, DB_PATH, TABLE_NAME, DUMMY_PARQUET_FILE_FOR_IMPORT

# Load your labellised dataframe (replace with your actual file path)
labellised_df = pd.read_parquet('chatbot_test_dataset_enriched.parquet')

# Initialize environment and LanceDB table
initialize_environment()
lancedb_table = setup_lancedb_table(
    db_path=DB_PATH,
    table_name=TABLE_NAME,
    parquet_file_path=DUMMY_PARQUET_FILE_FOR_IMPORT,
    id_column_parquet='item_id',
    vector_column_parquet='embedding',
    text_column_parquet='offer_description',
    index_type="vector"
)

# Function to extract item ids from LLM output
def extract_ids_from_llm_answer(llm_answer):
    ids = set()
    for line in llm_answer.splitlines():
        id_match = re.search(r"id\s*[:=\-]?\s*([\w\-]+)", line, re.IGNORECASE)
        if not id_match:
            id_match = re.match(r"\s*([\w\-]+)\s*[:=\-]", line)
        if id_match:
            ids.add(id_match.group(1))
    if not ids:
        ids = set(re.findall(r"\b(?:product|offer|item)-[\w\d]+\b", llm_answer, re.IGNORECASE))
    return ids

# For each unique tag_name, run RAG and compare
results = []
for tag_name in labellised_df['tag_name'].unique():
    print(f"\n--- Evaluating tag: {tag_name} ---")
    # Ground truth: all item_ids for this tag
    gt_ids = set(labellised_df[labellised_df['tag_name'] == tag_name]['item_id'].astype(str))
    # RAG answer
    llm_answer = rag_query(lancedb_table, tag_name, k_retrieval=50)
    pred_ids = extract_ids_from_llm_answer(llm_answer)
    # Compare
    intersection = gt_ids & pred_ids
    precision = len(intersection) / len(pred_ids) if pred_ids else 0
    recall = len(intersection) / len(gt_ids) if gt_ids else 0
    print(f"Ground truth IDs: {gt_ids}")
    print(f"Predicted IDs: {pred_ids}")
    print(f"Precision: {precision:.2f}, Recall: {recall:.2f}")
    results.append({
        'tag_name': tag_name,
        'precision': precision,
        'recall': recall,
        'n_gt': len(gt_ids),
        'n_pred': len(pred_ids),
        'n_intersection': len(intersection)
    })

# Save results to CSV
results_df = pd.DataFrame(results)
results_df.to_csv('rag_vs_labellised_results.csv', index=False)
print("\nResults saved to rag_vs_labellised_results.csv")
