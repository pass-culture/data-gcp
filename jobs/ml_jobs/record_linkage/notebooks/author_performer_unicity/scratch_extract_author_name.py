import pandas as pd

df = (
    pd.read_csv(
        "notebooks/author_performer_unicity/data/extract_bq_is_synchronized.csv"
    )
    .drop_duplicates(subset=["author"])
    .assign(author=lambda df: df.author.str.strip(", collectif"))
)

expected_df = pd.DataFrame(
    {
        "author": [
            "jean-luc godard",
            "godard, jean-luc",
            "visser-bourdon, patrick",
            "godard-p",
            "thirion-l+godard-p",
            "jean-luc godard, jean-pierre gorin",
            "godard, jean-luc ; gorin, jean-pierre",
            "godard/gorin",
            "s.bodard p-l.vallee",
            "godard, c.-e. ; godard,s. ; pinteaux, p.",
            "godard, charles-edouard;godard, severine",
            "bodak s. - caradec c",
            "godard ; bonnet ; mounier ; jarbinet ; rossi ; males ; blanc-dumont",
            "godard d'aucour-c",
            "zoltan kodaly & erno von dohnanyi",
            "angele baux godard",
            "godart g-l.",
            "agbodan-aolio, yann-cedr",
            "hristophe cazenove, françois vodarzac, cosby",
            "hristian godard, juan",
            "modak marianne, mess",
        ],
        "num_author": [1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 2, 2, 7, 1, 2, 1, 1, 2, 3, 2, 2],
    }
)

# test_df = expected_df[["author"]]
test_df = df[["author"]]

# %% Work with different patterns
result_df_list = []
matched_indexes_list = []
for pattern in [";", "/", " - ", "&", "\+"]:
    result_df_list.append(
        test_df.loc[lambda df: ~df.index.isin(matched_indexes_list)]
        .loc[lambda df: df.author.str.contains(pattern)]
        .assign(num_author=lambda df: df.author.str.split(pattern).map(len))
    )
    matched_indexes_list += result_df_list[-1].index.tolist()

# %% author separated by white blanks
pattern_abbreviated = r"^[a-z](-[a-z])*\.[a-z]+(\s[a-z](-[a-z])*\.[a-z]+)*$"
result_df_list.append(
    test_df.loc[lambda df: ~df.index.isin(matched_indexes_list)]
    .loc[lambda df: df.author.str.contains(pattern_abbreviated)]
    .assign(num_author=lambda df: df.author.str.split(" ").map(len))
)
matched_indexes_list += result_df_list[-1].index.tolist()

# %% author separated by multiple commas
pattern_multi_commas = r"(?:.*,){2,}"
result_df_list.append(
    test_df.loc[lambda df: ~df.index.isin(matched_indexes_list)]
    .loc[lambda df: df.author.str.contains(pattern_multi_commas)]
    .assign(num_author=lambda df: df.author.str.split(",").map(len))
)
matched_indexes_list += result_df_list[-1].index.tolist()

# %% multi-author separated by commas qui respectent le format Prénom(s) Nom
# pattern_comma_split = r"^[\w'-]+\s[\w'-]+(\s[\w'-]+)*(,\s[\w'-]+\s[\w'-]+(\s[\w'-]+)*)*$"
pattern_comma_split = (
    r"^[\.\w'-]+\s[\.\w'-]+(\s[\.\w'-]+)*(,\s[\.\w'-]+\s[\.\w'-]+(\s[\.\w'-]+)*)*$"
)

result_df_list.append(
    test_df.loc[lambda df: ~df.index.isin(matched_indexes_list)]
    .loc[lambda df: df.author.str.contains(pattern_comma_split)]
    .assign(num_author=lambda df: df.author.str.split(",").map(len))
)
matched_indexes_list += result_df_list[-1].index.tolist()
print(
    pd.DataFrame(
        {
            "text": pd.Series(
                [
                    "agbodan-aolio, yann-cedr",
                    "prodanova-thouvenin de strinava, svetoslava",
                    "jonathan hickman, mike deodato jr., dustin weaver, jerome opeña, adam kubert",
                    "déodat de séverac",
                    "jean-luc godard, jean-pierre gorin",
                    "kyo shirodaira, chashiba katase",
                    "godard, jean-luc",
                    "dsdq dsqd",
                    "sdsq-dsqd",
                    "sdsq-dsqd dsds",
                    "sdsq;dsqd",
                    "philippe gaubert, jacques ibert, déodat de séverac",
                ]
            )
        }
    ).assign(match=lambda df: df.text.str.contains(pattern_comma_split))
)

# %% Reconstruction
multi_author_df = pd.concat(result_df_list)
result_df = pd.concat(
    [
        multi_author_df,
        test_df.loc[lambda df: ~df.index.isin(matched_indexes_list)].assign(
            num_author=1
        ),
    ]
).sort_index()
print(result_df)

# %%
unmatched_df = test_df.loc[lambda df: ~df.index.isin(matched_indexes_list)].assign(
    num_author=1
)
print(unmatched_df)
