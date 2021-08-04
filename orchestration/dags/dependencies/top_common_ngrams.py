import pandas as pd
import nltk
import json
import yaml
import re
import os
from datetime import datetime
import gcsfs
from nltk.corpus import stopwords as StopWords
import collections
from dependencies.bigquery_client import BigQueryClient
from dependencies.config import (
    GCP_PROJECT,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_ANALYTICS_DATASET,
    DATA_GCS_BUCKET_NAME,
)

nltk.download("stopwords")
stopwords = StopWords.words("french")


def get_offers_name_to_tags():
    query = f"""SELECT offer_name, offer_id 
                FROM {GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.applicative_database_offer """
    offer_name_to_tag = pd.read_gbq(query)
    return offer_name_to_tag


def clean(name):
    name = re.sub(r"[^\w\s]", "", name.lower())
    return " ".join([w for w in name.split() if w not in stopwords])


def tokenize(string):
    """Convert string to lowercase and split into words (ignoring
    punctuation), returning list of words.
    """
    return re.findall(r"\w+", string.lower())


def count_ngrams(lines, min_length=2, max_length=4):
    """Iterate through given lines iterator (file object or list of
    lines) and return n-gram frequencies. The return value is a dict
    mapping the length of the n-gram to a collections.Counter
    object of n-gram tuple and number of times that n-gram occurred.
    Returned dict includes n-grams of length min_length to max_length.
    """
    lengths = range(min_length, max_length + 1)
    ngrams = {length: collections.Counter() for length in lengths}
    queue = collections.deque(maxlen=max_length)

    # Helper function to add n-grams at start of current queue to dict
    def add_queue():
        current = tuple(queue)
        for length in lengths:
            if len(current) >= length:
                ngrams[length][current[:length]] += 1

    # Loop through all lines and words and add n-grams to dict
    for line in lines:
        queue.clear()
        for word in tokenize(line):
            queue.append(word)
            if len(queue) >= max_length:
                add_queue()

    # Make sure we get the n-grams at the tail end of the queue
    while len(queue) > min_length:
        queue.popleft()
        add_queue()

    return ngrams


def get_most_frequent(ngrams, num=10):
    """Get num most common n-grams of each length in n-grams dict."""
    most_common = []  # List that contains the lists of most common n-grams for each n
    for n in sorted(ngrams):
        most_common_n = []  # List of most common n-grams for n
        for gram, count in ngrams[n].most_common(num):
            most_common_n.append(gram)
        most_common.append(most_common_n)

    return most_common


def save_result(most_common):
    fs = gcsfs.GCSFileSystem(project=GCP_PROJECT)
    with fs.open(
        f"gs://{DATA_GCS_BUCKET_NAME}/top_commons_ngrams/top_commons_ngrams.json", "w"
    ) as file:
        yaml.dump(most_common, file)


def save_top_common(min_length=2, max_length=4, num=10):
    # Data recovery
    df = get_offers_name_to_tags()
    df["offer_name_clean_stop"] = df["offer_name"].apply(lambda s: clean(s))
    # Creation of the dictionnary containing all the n-grams
    ngrams = count_ngrams(df["offer_name_clean_stop"], max_length=max_length)
    # Creation of the list containing all the most common n-grams
    most_common = get_most_frequent(ngrams, num=num)
    # Save top commons ngrams
    save_result(most_common)
