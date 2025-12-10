import numpy as np
import pandas as pd

VECTOR_DIM = 3
VECTOR_SIZE = 20
TRANSFORMER = "hf-internal-testing/tiny-random-camembert"
DETAIL_COLUMNS = ["vector", "booking_number_desc"]

STATIC_FAKE_ITEM_DATA = [
    {
        "item_id": 1,
        "booking_number_desc": 1,
        "title": "The Great Gatsby",
        "price": 15.0,
    },
    {"item_id": 2, "booking_number_desc": 2, "title": "Inception", "price": 20.0},
    {
        "item_id": 3,
        "booking_number_desc": 3,
        "title": "The Silent Patient",
        "price": 12.5,
    },
    {
        "item_id": 4,
        "booking_number_desc": 4,
        "title": "The Shawshank Redemption",
        "price": 25.0,
    },
    {"item_id": 5, "booking_number_desc": 5, "title": "Naruto T1", "price": 25.0},
    {
        "item_id": 6,
        "booking_number_desc": 6,
        "title": "The story of a Book",
        "price": 25.0,
    },
]


# Corrected dot product calculation
def calculate_dot_product(query_vector, static_vectors: pd.DataFrame):
    """Calculate dot product between query vector and each vector in static_vectors."""
    query_vector = np.array(query_vector)  # Ensure query_vector is a NumPy array
    return [
        np.dot(query_vector, -np.array(row["vector"]))
        for _, row in static_vectors.iterrows()
    ]
