import string

import pandas as pd

author_df = (
    pd.read_csv("notebooks/author_performer_unicity/data/author.csv")
    .drop_duplicates(subset=["author"])
)

#%%
author_df.offer_category_id.value_counts()

#%% Cinema
to_remove = ["avec" , "bande annonce"]
    # Les avec en string
    #Les trucs entre parenthèse
    # Les "bande annonce"
punctuation = r"[!\"#$%&'()*+,-./:;<=>?@[\]^_`{|}~]"
split_punctuation = [",", "|", " - ", "/", "&"]
cinema_authors_df = author_df.loc[lambda df: df.offer_category_id == "CINEMA"]
# punctuation_df = cinema_authors_df.loc[lambda df: df.author.str.contains("[,|&]{1,}")]
punctuation_df = cinema_authors_df.loc[lambda df: df.author.str.contains(punctuation)]
