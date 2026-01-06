import pandas as pd


def panachage_sort(df: pd.DataFrame) -> pd.DataFrame:
    # Sort by rank and offer_id for deterministic order
    df_sorted = df.sort_values(["rank", "offer_id"]).reset_index(drop=True)
    # Group by rank
    groups = [g for _, g in df_sorted.groupby("rank")]
    # Find the maximum group length
    max_len = max(len(g) for g in groups)
    # Interleave rows: one from each group, then repeat
    rows = []
    for i in range(max_len):
        for g in groups:
            if i < len(g):
                rows.append(g.iloc[i])
    return pd.DataFrame(rows).reset_index(drop=True)
