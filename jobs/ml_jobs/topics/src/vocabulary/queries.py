"""The item_metadata query vocabulary modelling reads from (spec section 5).

``gtl_type`` is also selected (not just filtered on) so
``src.vocabulary.filtering.filter_book_documents``'s own defensive check has
the column to look at -- the WHERE clause already guarantees every row is
``'BOOK'``.
"""

from src.common.constants import BIGQUERY_ML_INPUT_DATASET, GCP_PROJECT_ID

ITEM_METADATA_QUERY = f"""
select
    item_id,
    offer_name,
    offer_description,
    gtl_type,
    titelive_gtl_id as gtl_id,
    gtl_label_level_1,
    gtl_label_level_2,
    gtl_label_level_3,
    gtl_label_level_4
from `{GCP_PROJECT_ID}.{BIGQUERY_ML_INPUT_DATASET}.item_metadata`
where gtl_type = 'BOOK' and titelive_gtl_id is not null
""".strip()
