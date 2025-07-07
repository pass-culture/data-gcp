import time

from archiving import ListArchive, MoveToArchive
from metabase_api import MetabaseAPI
from utils import (
    CLIENT_ID,
    METABASE_API_USERNAME,
    METABASE_HOST,
    PASSWORD,
    max_cards_to_archive,
    parent_folder_to_archive,
    rules,
)


def run():
    metabase = MetabaseAPI(
        username=METABASE_API_USERNAME,
        password=PASSWORD,
        host=METABASE_HOST,
        client_id=CLIENT_ID,
    )

    for folder_to_archive in parent_folder_to_archive:
        folder_rules = [
            rule
            for rule in rules
            if rule["rule_name"] == f"{folder_to_archive}_archiving"
        ]
        list_archive = ListArchive(
            metabase_folder=folder_to_archive, rules=folder_rules
        )
        list_archive.get_data_archiving()
        archives_dicts = list_archive.preprocess_data_archiving(object_type="card")

        for card in archives_dicts[:max_cards_to_archive]:
            archiving = MoveToArchive(
                movement=card,
                metabase=metabase,
            )
            archiving.move_object()
            archiving.rename_archive_object()
            archiving.save_logs_bq()
            time.sleep(1)

    return "success"


run()
