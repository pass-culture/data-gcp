import pandas as pd


def preprocess_offer_linkage(unmatched_elements_path):
    # Load the unmatched elements
    unmatched_elements = pd.read_parquet(unmatched_elements_path)
    # Filter the unmatched elements
    unmatched_elements = unmatched_elements[unmatched_elements["type"] == "offer"]
    # Drop the type column
    unmatched_elements = unmatched_elements.drop(columns=["type"])
    # Return the filtered unmatched elements
    return unmatched_elements
