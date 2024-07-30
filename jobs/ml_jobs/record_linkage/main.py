import concurrent
import traceback
from itertools import repeat
from multiprocessing import cpu_count
from loguru import logger
import pandas as pd
import recordlinkage
from tools.config import (
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    SUBCATEGORIES_WITH_PERFORMER,
    data_and_hyperparams_dict,
)
import typer
from tools.linkage import get_linked_offers, get_linked_offers_from_graph


def process_record_linkage(
    indexer,
    data_and_hyperparams_dict,
    df_source_tmp,
    subset_divisions,
    batch_number,
    batch_id,
):
    try:
        return get_linked_offers(
            indexer,
            data_and_hyperparams_dict,
            df_source_tmp,
            subset_divisions,
            batch_number,
            batch_id,
        )
    except Exception as e:
        print(e)
        traceback.print_exc()
        return False


def multiprocess_linkage(
    max_process, indexer, data_and_hyperparams_dict_tmp, df_source_tmp
):
    subset_length = len(df_source_tmp) // max_process
    subset_length = subset_length if subset_length > 0 else 1
    batch_number = max_process if subset_length > 1 else 1
    print(f"Starting process with {batch_number} CPUs, subset length: {subset_length} ")
    # batch_urls = [list(chunk) for chunk in list(np.array_split(urls, batch_number))]

    with concurrent.futures.ProcessPoolExecutor(batch_number) as executor:
        futures = executor.map(
            process_record_linkage,
            repeat(indexer),
            repeat(data_and_hyperparams_dict_tmp),
            repeat(df_source_tmp),
            repeat(subset_length),
            repeat(batch_number),
            range(batch_number),
        )
    print("Multiprocessing done")
    return futures


def main(
    gcp_project: str = typer.Option(
        GCP_PROJECT_ID,
        help="BigQuery Project in which the offers to link is located",
    ),
    input_dataset_name: str = typer.Option(..., help="Path to the dataset input name."),
    input_table_name: str = typer.Option(..., help="Path to the intput table name."),
    output_dataset_name: str = typer.Option(..., help="Path to the dataset name."),
    output_table_name: str = typer.Option(..., help="Path to the output table name."),
) -> None:
    ###############
    # Load preprocessed data
    logger.info("Loading offers to link...")
    df_offers_to_link_clean = pd.read_gbq(
        f"SELECT * FROM `{gcp_project}.{input_dataset_name}{input_table_name}`"
    )
    logger.info(f"{len(df_offers_to_link_clean)} items to link")
    ###############
    # Split offers between performer and non performer
    logger.info("Split offers between offers with performer and non performer")
    subcat_all = df_offers_to_link_clean.offer_subcategoryId.drop_duplicates().to_list()
    subcat_wo_performer = [
        x for x in subcat_all if x not in SUBCATEGORIES_WITH_PERFORMER
    ]

    df_to_link_performer = df_offers_to_link_clean.query(
        f"""offer_subcategoryId in {tuple(SUBCATEGORIES_WITH_PERFORMER)} """
    )

    df_to_link_non_performer = df_offers_to_link_clean.query(
        f"""offer_subcategoryId in {tuple(subcat_wo_performer)} """
    )
    logger.info(f"{len(df_to_link_non_performer)} items without performer to link")
    logger.info(f"{len(df_to_link_performer)} items with performer to link")
    ###############
    # Add dataframe to link to analysis config dict
    data_and_hyperparams_dict["performer"]["dataframe_to_link"] = df_to_link_performer
    data_and_hyperparams_dict["non_performer"][
        "dataframe_to_link"
    ] = df_to_link_non_performer
    ###############
    # Run linkage for each group (performer, non-performer) then concat both dataframe to get linkage on full data
    max_process = cpu_count() - 1
    offers_matched_by_group_df_list = []
    indexer = recordlinkage.Index()
    indexer.full()
    for group_sample in data_and_hyperparams_dict.keys():
        data_and_hyperparams_dict_tmp = data_and_hyperparams_dict[group_sample]
        df_source = data_and_hyperparams_dict_tmp["dataframe_to_link"].copy()
        if len(df_source) > 0:
            offers_matched_by_subcat_df_list = []
            for subcat in df_source.offer_subcategoryId.unique():
                offers_matched_df_list = []
                print("Linkage for subcat: ", subcat, " on going ...")
                df_source_tmp = df_source.query(f"offer_subcategoryId=='{subcat}'")
                logger.info(f"{len(df_source_tmp)} offers to link")
                if len(df_source_tmp):
                    futures = multiprocess_linkage(
                        max_process,
                        indexer,
                        data_and_hyperparams_dict_tmp,
                        df_source_tmp,
                    )
                    for future in futures:
                        offers_matched_df_list.append(future)
                    df_offers_matched = get_linked_offers_from_graph(
                        df_source_tmp, pd.concat(offers_matched_df_list)
                    )
                    offers_matched_by_subcat_df_list.append(df_offers_matched)
                if len(offers_matched_by_subcat_df_list) > 0:
                    offers_matched_by_group_df_list.append(
                        pd.concat(offers_matched_by_subcat_df_list)
                    )
    df_offers_linked_full = pd.concat(offers_matched_by_group_df_list)

    df_offers_linked_full = df_offers_linked_full.drop_duplicates()
    df_offers_linked_full.to_gbq(
        f"{output_dataset_name}.{output_table_name}",
        project_id=gcp_project,
        if_exists="replace",
    )


if __name__ == "__main__":
    main(GCP_PROJECT_ID, ENV_SHORT_NAME)
