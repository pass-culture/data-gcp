import pandas as pd
import typer

app = typer.Typer()


### Params
def compute_metrics_per_slice(
    artists_per_slice: pd.api.typing.DataFrameGroupBy,
) -> pd.Series:
    tp = artists_per_slice.loc[artists_per_slice.tp].offer_number.sum()
    fp = artists_per_slice.loc[artists_per_slice.fp].offer_number.sum()
    fn = artists_per_slice.loc[artists_per_slice.fn].offer_number.sum()
    tn = artists_per_slice.loc[artists_per_slice.tn].offer_number.sum()

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = (
        2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
    )

    return pd.Series(
        {
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "tn": tn,
            "precision": precision,
            "recall": recall,
            "f1": f1,
        }
    )


def get_main_matched_cluster_per_dataset(
    matched_artists_in_test_set_df: pd.DataFrame,
) -> dict:
    return (
        (
            matched_artists_in_test_set_df.loc[lambda df: df.is_my_artist]
            .groupby(["dataset_name", "cluster_id"])
            .agg(
                {
                    "offer_number": "sum",
                }
            )
            .sort_values(["dataset_name", "offer_number"], ascending=[True, False])
        )
        .reset_index()
        .drop_duplicates(subset=["dataset_name"], keep="first")
        .drop(columns=["offer_number"])
        .set_index("dataset_name")
        .to_dict()["cluster_id"]
    )


@app.command()
def main(
    input_file_path: str = typer.Option(),
    output_file_path: str = typer.Option(),
) -> None:
    matched_artists_in_test_set_df = pd.read_parquet(input_file_path)

    main_cluster_per_dataset = get_main_matched_cluster_per_dataset(
        matched_artists_in_test_set_df
    )

    metrics_per_dataset_df = (
        matched_artists_in_test_set_df.assign(
            is_main_cluster=lambda df: df.cluster_id.isin(
                main_cluster_per_dataset.values()
            ),
            tp=lambda df: df.is_my_artist & df.is_main_cluster,
            fp=lambda df: ~df.is_my_artist & df.is_main_cluster,
            fn=lambda df: df.is_my_artist & ~df.is_main_cluster,
            tn=lambda df: ~df.is_my_artist & ~df.is_main_cluster,
        )
        .groupby("dataset_name")
        .apply(compute_metrics_per_slice)
        .reset_index()
    )

    metrics_per_dataset_df.to_parquet(output_file_path, index=False)


if __name__ == "__main__":
    app()
