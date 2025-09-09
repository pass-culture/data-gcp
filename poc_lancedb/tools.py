import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger
# 3. Use real Parquet file if it exists, otherwise generate dummy data
def create_dummy_table(parquet_file_path, data_size=500, vector_dim=384):
    """
    Ensures that the specified Parquet file exists. If not, generates dummy data and writes it to the file.
    """
    logger.warning(f"Parquet file '{parquet_file_path}' not found. Generating dummy data.")
    ids = [f"item_{i:03d}" for i in range(data_size)]
    vectors = np.random.rand(data_size, vector_dim).astype(np.float32)
    texts = [
        f"This is a dummy text description for item {i}. It covers topics like data science, AI, and machine learning. This is more text to make it longer and more diverse."
        for i in range(data_size)
    ]

    # Use PyArrow to create a FixedSizeListArray for the vector column
    
    ids_array = pa.array(ids, pa.string())
    texts_array = pa.array(texts, pa.string())
    vector_flat = vectors.flatten().tolist()
    vector_value_array = pa.array(vector_flat, pa.float32())
    vector_list_array = pa.FixedSizeListArray.from_arrays(vector_value_array, vector_dim)

    table = pa.table({
        'item_id': ids_array,
        'embedding': vector_list_array,
        'offer_description': texts_array
    })
    # Write to Parquet
    pq.write_table(table, parquet_file_path)
    logger.info(f"Generated dummy parquet file for direct import: {parquet_file_path} with {data_size} records.")
