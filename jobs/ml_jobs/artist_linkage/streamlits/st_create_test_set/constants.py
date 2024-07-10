from enum import Enum

TEST_SET_TO_LABELIZE_GCS_PATH = (
    "gs://data-bucket-prod/link_artists/test_sets_to_labelize"
)
TEST_SET_GCS_PATH = "gs://data-bucket-prod/link_artists/labelized_test_sets"


class LabellingStatus(Enum):
    OK = "is_my_artist"
    IRRELEVANT = "irrelevant_data"
